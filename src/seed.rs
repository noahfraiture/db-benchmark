#[derive(Debug, Clone)]
pub struct UserSeed {
    pub email: String,
    pub country: String,
    pub segment: String,
}

#[derive(Debug, Clone)]
pub struct UserProfileSeed {
    pub user_idx: usize,
    pub bio: String,
    pub timezone: String,
}

#[derive(Debug, Clone)]
pub struct OrderSeed {
    pub user_idx: usize,
    pub total_cents: i64,
    pub status: &'static str,
}

#[derive(Debug, Clone)]
pub struct OrderItemSeed {
    pub order_idx: usize,
    pub sku: String,
    pub quantity: i32,
    pub unit_price_cents: i64,
}

#[derive(Debug, Clone)]
pub struct PaymentSeed {
    pub order_idx: usize,
    pub amount_cents: i64,
    pub status: &'static str,
}

#[derive(Debug, Clone)]
pub struct EventSeed {
    pub user_idx: usize,
    pub ts_unix_secs: i64,
    pub metric_name: &'static str,
    pub value: f64,
}

#[derive(Debug, Clone)]
pub struct Seeds {
    pub users: Vec<UserSeed>,
    pub profiles: Vec<UserProfileSeed>,
    pub orders: Vec<OrderSeed>,
    pub order_items: Vec<OrderItemSeed>,
    pub payments: Vec<PaymentSeed>,
    pub events: Vec<EventSeed>,
}

pub fn generate_seeds(rows: usize, event_rows: usize) -> Seeds {
    let countries = ["US", "DE", "FR", "JP", "BR"];
    let segments = ["free", "pro", "enterprise"];
    let timezones = ["UTC", "America/New_York", "Europe/Berlin", "Asia/Tokyo"];

    let users = (0..rows)
        .map(|i| UserSeed {
            email: format!("user{i}@example.com"),
            country: countries[i % countries.len()].to_string(),
            segment: segments[i % segments.len()].to_string(),
        })
        .collect::<Vec<_>>();

    let profiles = (0..rows)
        .map(|i| UserProfileSeed {
            user_idx: i,
            bio: format!("Bio for user {i}"),
            timezone: timezones[i % timezones.len()].to_string(),
        })
        .collect::<Vec<_>>();

    let orders = (0..rows)
        .map(|i| OrderSeed {
            user_idx: i,
            total_cents: 500 + (i % 200) as i64 * 100,
            status: if i % 5 == 0 { "pending" } else { "paid" },
        })
        .collect::<Vec<_>>();

    let order_items = (0..rows * 3)
        .map(|i| OrderItemSeed {
            order_idx: i % rows,
            sku: format!("SKU-{:04}", i % 150),
            quantity: (i % 4 + 1) as i32,
            unit_price_cents: 100 + ((i * 7) % 20) as i64 * 75,
        })
        .collect::<Vec<_>>();

    let payments = (0..rows)
        .filter(|i| i % 4 != 0)
        .map(|i| PaymentSeed {
            order_idx: i,
            amount_cents: 500 + (i % 200) as i64 * 100,
            status: if i % 9 == 0 { "failed" } else { "captured" },
        })
        .collect::<Vec<_>>();

    let base_ts = 1_735_689_600_i64;
    let events = (0..event_rows)
        .map(|i| EventSeed {
            user_idx: i % rows.max(1),
            ts_unix_secs: base_ts + (i as i64 * 60),
            metric_name: if i % 2 == 0 { "cpu" } else { "latency" },
            value: 20.0 + ((i * 13) % 700) as f64 / 10.0,
        })
        .collect::<Vec<_>>();

    Seeds {
        users,
        profiles,
        orders,
        order_items,
        payments,
        events,
    }
}
