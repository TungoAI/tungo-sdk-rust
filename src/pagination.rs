//! Cursor pagination: typed pages and auto-paging streams.

use std::collections::{HashSet, VecDeque};
use std::ops::Deref;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use serde::de::DeserializeOwned;

use crate::client::Tungo;
use crate::error::{Error, Result};
use crate::request::Request;
use crate::response::{RateLimit, Response, ResponseMeta};

/// A list response of the form `{ data, next_cursor }`.
///
/// It is implemented by every page model of the API.
pub trait CursorPage: DeserializeOwned {
    /// The type of one item of the list.
    type Item;

    /// The items of this page.
    fn items(&self) -> &[Self::Item];

    /// Takes the items out of the page.
    fn into_items(self) -> Vec<Self::Item>;

    /// The cursor of the next page; `None` on the last page.
    fn next_cursor(&self) -> Option<&str>;
}

/// One page of a list, able to fetch the page after it.
///
/// It dereferences to the page model, so `page.data` and any other field of
/// the model are read directly.
///
/// ```no_run
/// # use tungo_sdk::Tungo;
/// # async fn run(client: Tungo) -> tungo_sdk::Result<()> {
/// let mut page = client.chat().list(&Default::default()).await?;
/// loop {
///     for dialog in &page.data {
///         println!("{}", dialog.id);
///     }
///     match page.next_page().await? {
///         Some(next) => page = next,
///         None => break,
///     }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Page<P> {
    client: Tungo,
    request: Request,
    page: P,
    meta: ResponseMeta,
    /// The cursors already sent on the way to this page.
    seen: Vec<String>,
}

impl<P: CursorPage> Page<P> {
    pub(crate) fn new(client: Tungo, request: Request, response: Response<P>) -> Self {
        let (page, meta) = response.into_parts();
        // The cursor the caller put into the first request counts as sent.
        let seen = request
            .query
            .iter()
            .filter(|(name, _)| name == "cursor")
            .map(|(_, value)| value.clone())
            .collect();
        Page {
            client,
            request,
            page,
            meta,
            seen,
        }
    }

    /// The cursor of the next page; `None` on the last page.
    pub fn next_cursor(&self) -> Option<&str> {
        self.page.next_cursor().filter(|cursor| !cursor.is_empty())
    }

    /// Whether there is a page after this one.
    pub fn has_next(&self) -> bool {
        self.next_cursor().is_some()
    }

    /// Fetches the next page with the same filters; `None` after the last page.
    pub async fn next_page(&self) -> Result<Option<Page<P>>> {
        let Some(cursor) = self.next_cursor() else {
            return Ok(None);
        };
        // Every cursor sent since the first page is remembered, so a server
        // that cycles (A, B, A, ...) is caught as well as one that repeats.
        if self.seen.iter().any(|sent| sent == cursor) {
            return Err(repeated_cursor(cursor));
        }
        let mut request = self.request.clone();
        request.set_query("cursor", cursor);
        let response = self.client.execute::<P>(request.clone()).await?;
        let mut next = Page::new(self.client.clone(), request, response);
        next.seen.clone_from(&self.seen);
        next.seen.push(cursor.to_owned());
        Ok(Some(next))
    }

    /// The page model.
    pub fn into_inner(self) -> P {
        self.page
    }

    /// The items of this page.
    pub fn into_items(self) -> Vec<P::Item> {
        self.page.into_items()
    }

    /// The status and headers of the response that carried this page.
    pub fn meta(&self) -> &ResponseMeta {
        &self.meta
    }

    /// The `x-request-id` of the response.
    pub fn request_id(&self) -> Option<&str> {
        self.meta.request_id()
    }

    /// The rate-limit state of the response.
    pub fn rate_limit(&self) -> Option<RateLimit> {
        self.meta.rate_limit()
    }
}

impl<P> Deref for Page<P> {
    type Target = P;

    fn deref(&self) -> &P {
        &self.page
    }
}

fn repeated_cursor(cursor: &str) -> Error {
    Error::Pagination(format!(
        "the server returned the cursor `{cursor}` again; stopping instead of looping forever"
    ))
}

struct Walk {
    client: Tungo,
    request: Option<Request>,
    failure: Option<Error>,
    /// Every cursor sent so far.
    seen: HashSet<String>,
}

/// A lazy stream over the items of every page of a list.
///
/// A page is requested only when the items of the previous one are used up,
/// so leaving the loop early stops fetching. The filters of the first
/// request are carried to the following pages.
///
/// ```no_run
/// use futures_util::TryStreamExt;
///
/// # use tungo_sdk::Tungo;
/// # async fn run(client: Tungo) -> tungo_sdk::Result<()> {
/// let mut dialogs = client.chat().list_all(&Default::default());
/// while let Some(dialog) = dialogs.try_next().await? {
///     println!("{}", dialog.id);
/// }
/// # Ok(())
/// # }
/// ```
pub struct ItemStream<T> {
    inner: Pin<Box<dyn Stream<Item = Result<T>> + Send>>,
}

impl<T: Send + 'static> ItemStream<T> {
    pub(crate) fn new<P>(client: Tungo, request: Result<Request>) -> Self
    where
        P: CursorPage<Item = T> + Send + 'static,
    {
        let (request, failure) = match request {
            Ok(request) => (Some(request), None),
            Err(error) => (None, Some(error)),
        };
        let walk = Walk {
            client,
            request,
            failure,
            seen: HashSet::new(),
        };
        let pages = futures_util::stream::unfold(walk, |mut walk| async move {
            if let Some(error) = walk.failure.take() {
                return Some((Err(error), walk));
            }
            let request = walk.request.take()?;
            let response = match walk.client.execute::<P>(request.clone()).await {
                Ok(response) => response,
                Err(error) => return Some((Err(error), walk)),
            };
            let page = response.into_inner();
            if let Some(cursor) = page.next_cursor().filter(|cursor| !cursor.is_empty()) {
                if walk.seen.insert(cursor.to_owned()) {
                    let mut next = request;
                    next.set_query("cursor", cursor);
                    walk.request = Some(next);
                } else {
                    walk.failure = Some(repeated_cursor(cursor));
                }
            }
            Some((Ok(VecDeque::from(page.into_items())), walk))
        });
        ItemStream {
            inner: Box::pin(Flatten {
                pages: Box::pin(pages),
                items: VecDeque::new(),
            }),
        }
    }
}

impl<T> Stream for ItemStream<T> {
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl<T> std::fmt::Debug for ItemStream<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ItemStream").finish_non_exhaustive()
    }
}

/// Yields the items of each page in turn.
struct Flatten<T> {
    pages: Pin<Box<dyn Stream<Item = Result<VecDeque<T>>> + Send>>,
    items: VecDeque<T>,
}

impl<T> Unpin for Flatten<T> {}

impl<T> Stream for Flatten<T> {
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(item) = self.items.pop_front() {
                return Poll::Ready(Some(Ok(item)));
            }
            match self.pages.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(Some(Ok(items))) => self.items = items,
            }
        }
    }
}
