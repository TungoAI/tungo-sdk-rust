# Tungo AI Rust SDK (`tungo-sdk`)

Официальный Rust SDK для публичного API платформы **Tungo AI** (Rust 2021, **0 внешних зависимостей**).

## Быстрый старт

```rust
use tungo_sdk::{AskInput, TungoClient, TungoMoney};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = TungoClient::builder()
        .api_key("tg_live_...")
        .company_id("co_...")
        .build();

    let ans = client.ask(AskInput {
        prompt: "Покажи дебиторку за октябрь".to_string(),
        company_id: None,
        conversation_id: None,
    })?;
    println!("{}", ans.reply);
    println!("{}", TungoMoney::format_tiyn(TungoMoney::to_tiyn(45_000.0), "KZT"));
    Ok(())
}
```

## Полный справочник ресурсов и методов

<!-- sdkgen:methods -->
Автоматически сгенерировано из OpenAPI-контракта Tungo Platform API: **13 ресурсов, 82 метода, 22 вебхука**.

| Ресурс | Описание | Методы |
| --- | --- | --- |
| `client.workspaces()` | Организации (workspace), доступные ключу API | `list` |
| `client.companies()` | Компании организации и их карточки | `list` · `get` |
| `client.platform()` | Баланс платформы, книга движений, отчёты о расходе, лимиты, аудит и тарифы моделей | `list_usage_rates` · `get_account` · `get_audit / get_audit_all` · `get_ledger / get_ledger_all` · `list_limits` · `list_requests / list_requests_all` · `get_usage` |
| `client.api_keys()` | Сведения о текущем ключе API и статистика его расхода | `get_current` · `get_current_usage` · `get_usage` |
| `client.webhooks()` | Исходящие вебхуки организации (Svix): адреса, каталог событий, доставки и проверка подписи | `list_endpoints / list_endpoints_all` · `create_endpoint` · `get_endpoint` · `update_endpoint` · `delete_endpoint` · `list_endpoint_attempts / list_endpoint_attempts_all` · `rotate_endpoint_secret` · `test_endpoint` · `list_event_types` · `list_messages / list_messages_all` · `list_message_attempts / list_message_attempts_all` · `resend_message` · `get_usage` |
| `client.chat()` | Диалоги с ИИ-бухгалтером: вопросы, потоковый чат (SSE), подсчёт токенов и история | `list / list_all` · `create / create_stream` · `count_tokens` · `get` · `update` · `delete` · `answer / answer_stream` · `list_events` · `stop` |
| `client.proposals()` | Предложения документов ИИ-бухгалтера: просмотр, одобрение записи в 1С и отклонение | `list / list_all` · `approve_batch` · `get` · `approve` · `reject` |
| `client.feed()` | Лента событий и предложений компании (страницы и живой поток SSE) | `get / get_all` · `list_events` |
| `client.batches()` | Пачки обработки банковских выписок, входящих ЭСФ и DirectBank | `list / list_all` · `create_bank_statement` · `create_esf_purchases` · `get` · `list_events` · `stop` · `get_summary` · `list_statements / list_statements_all` · `poll_directbank` |
| `client.esf()` | Входящие и исходящие электронные счета-фактуры (ИС ЭСФ) | `get_incoming / get_incoming_all` · `get_outgoing / get_outgoing_all` |
| `client.forms()` | Налоговые формы (ФНО 100.00 и др.): сборка, проверка ИСНА и разрывы данных | `list` · `build` · `list_data_gaps` · `get_isna` · `list_notes` |
| `client.onec()` | Состояние подключения компании к базе 1С | `get_status` |
| `client.automations()` | Автоматизации компании: входящие хуки, сценарии, цепочки шагов, запуски и уровень автономности ИИ | `post_hook` · `list_hooks` · `create_hook` · `get_hook` · `update_hook` · `delete_hook` · `rotate_hook` · `test_hook` · `list_runs / list_runs_all` · `get_run` · `cancel_run` · `retry_run` · `list_scenarios` · `create_scenario` · `get_scenario` · `put_scenario` · `update_scenario` · `delete_scenario` · `run_scenario` · `get_state` · `put_state` · `get_autonomy` · `put_autonomy` |
<!-- /sdkgen:methods -->
