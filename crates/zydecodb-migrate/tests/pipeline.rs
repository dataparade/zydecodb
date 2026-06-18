//! End-to-end pipeline test: `parse -> graph -> classify -> convert`.
//!
//! The inline unit tests in each module exercise one stage in isolation. This
//! integration test drives the whole transformation on a single realistic dump
//! and asserts the *assembled documents* and the dropped-constraint report —
//! the behavior an operator actually observes. It locks in every transformation
//! type at once (1:1 object embed, 1:N array embed, multi-FK snapshot, join-table
//! dissolution, single- and composite-PK `_id`) so a regression in any stage
//! that survives the unit tests still trips here.

use serde_json::Value;
use zydecodb_migrate::classify::{self, IdStrategy};
use zydecodb_migrate::convert::build_collection_docs;
use zydecodb_migrate::{graph, pgdump};

/// A shop schema touching every transformation the classifier can make:
/// - `profiles` is 1:1 with `users` (UNIQUE fk) -> embedded object
/// - `addresses` is bounded 1:N under `users` -> embedded array
/// - `order_items` is owned by `orders` and also points at the shared
///   `products` -> embedded array with a product snapshot
/// - `product_tags` is a composite-PK join table -> dissolved into `products`
/// - `users`/`products`/`tags`/`orders` survive as referenced collections
const SHOP: &str = r#"
SET statement_timeout = 0;

CREATE TABLE public.users (
    id integer NOT NULL,
    email text NOT NULL,
    name text NOT NULL
);
CREATE TABLE public.profiles (
    id integer NOT NULL,
    user_id integer NOT NULL,
    bio text
);
CREATE TABLE public.addresses (
    id integer NOT NULL,
    user_id integer NOT NULL,
    city text NOT NULL
);
CREATE TABLE public.tags (
    id integer NOT NULL,
    label text NOT NULL
);
CREATE TABLE public.products (
    id integer NOT NULL,
    sku text NOT NULL,
    name text NOT NULL,
    price numeric(10,2) NOT NULL
);
CREATE TABLE public.product_tags (
    product_id integer NOT NULL,
    tag_id integer NOT NULL
);
CREATE TABLE public.orders (
    id integer NOT NULL,
    user_id integer NOT NULL,
    placed_at timestamp without time zone
);
CREATE TABLE public.order_items (
    id integer NOT NULL,
    order_id integer NOT NULL,
    product_id integer NOT NULL,
    qty integer NOT NULL
);

COPY public.users (id, email, name) FROM stdin;
1	ada@x.io	Ada
2	bo@x.io	Bo
\.
COPY public.profiles (id, user_id, bio) FROM stdin;
10	1	hi
11	2	yo
\.
COPY public.addresses (id, user_id, city) FROM stdin;
100	1	London
101	1	Paris
102	2	NOLA
\.
COPY public.tags (id, label) FROM stdin;
1	red
2	blue
\.
COPY public.products (id, sku, name, price) FROM stdin;
50	W1	Widget	9.99
51	G1	Gadget	19.99
\.
COPY public.product_tags (product_id, tag_id) FROM stdin;
50	1
50	2
51	1
\.
COPY public.orders (id, user_id, placed_at) FROM stdin;
1000	1	2021-01-01 00:00:00
1001	1	2021-02-01 00:00:00
1002	2	2021-03-01 00:00:00
\.
COPY public.order_items (id, order_id, product_id, qty) FROM stdin;
1	1000	50	2
2	1000	51	1
3	1001	50	5
4	1002	51	3
5	1002	50	1
\.

ALTER TABLE ONLY public.users ADD CONSTRAINT users_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.users ADD CONSTRAINT users_email_key UNIQUE (email);
ALTER TABLE ONLY public.profiles ADD CONSTRAINT profiles_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.profiles ADD CONSTRAINT profiles_user_key UNIQUE (user_id);
ALTER TABLE ONLY public.addresses ADD CONSTRAINT addresses_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.tags ADD CONSTRAINT tags_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.products ADD CONSTRAINT products_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.products ADD CONSTRAINT products_sku_key UNIQUE (sku);
ALTER TABLE ONLY public.product_tags ADD CONSTRAINT product_tags_pkey PRIMARY KEY (product_id, tag_id);
ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.order_items ADD CONSTRAINT order_items_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.profiles ADD CONSTRAINT profiles_user_fkey FOREIGN KEY (user_id) REFERENCES public.users(id);
ALTER TABLE ONLY public.addresses ADD CONSTRAINT addr_user_fkey FOREIGN KEY (user_id) REFERENCES public.users(id);
ALTER TABLE ONLY public.product_tags ADD CONSTRAINT pt_prod_fkey FOREIGN KEY (product_id) REFERENCES public.products(id);
ALTER TABLE ONLY public.product_tags ADD CONSTRAINT pt_tag_fkey FOREIGN KEY (tag_id) REFERENCES public.tags(id);
ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_user_fkey FOREIGN KEY (user_id) REFERENCES public.users(id);
ALTER TABLE ONLY public.order_items ADD CONSTRAINT oi_order_fkey FOREIGN KEY (order_id) REFERENCES public.orders(id);
ALTER TABLE ONLY public.order_items ADD CONSTRAINT oi_prod_fkey FOREIGN KEY (product_id) REFERENCES public.products(id);
"#;

fn build_plan() -> (pgdump::Dump, classify::Plan) {
    let dump = pgdump::parse(SHOP).expect("dump parses");
    let g = graph::build(&dump);
    let plan = classify::classify(&dump, &g);
    (dump, plan)
}

/// Look up one collection's documents keyed by `_id`.
fn docs_by_id(dump: &pgdump::Dump, plan: &classify::Plan, coll: &str) -> Vec<(String, Value)> {
    let cp = plan.collection(coll).expect("collection survives");
    build_collection_docs(dump, cp)
        .expect("docs build")
        .into_iter()
        .map(|d| {
            (
                d.id,
                serde_json::from_slice(&d.body).expect("valid json body"),
            )
        })
        .collect()
}

#[test]
fn plan_shape_classifies_every_role() {
    let (_dump, plan) = build_plan();

    let surviving: Vec<&str> = plan.collections.iter().map(|c| c.name.as_str()).collect();
    for expected in ["users", "products", "tags", "orders"] {
        assert!(surviving.contains(&expected), "{expected} should survive");
    }
    // Folded-away tables are not collections.
    for gone in ["profiles", "addresses", "order_items", "product_tags"] {
        assert!(plan.collection(gone).is_none(), "{gone} should be folded");
    }

    assert!(plan.embedded_tables.contains(&"profiles".to_string()));
    assert!(plan.embedded_tables.contains(&"addresses".to_string()));
    assert!(plan.embedded_tables.contains(&"order_items".to_string()));
    assert!(plan.join_tables.contains(&"product_tags".to_string()));
}

#[test]
fn user_doc_embeds_one_to_one_profile_object_and_address_array() {
    let (dump, plan) = build_plan();
    let users = docs_by_id(&dump, &plan, "users");
    let (_id, ada) = users.iter().find(|(id, _)| id == "1").expect("user 1");

    assert_eq!(ada["_id"], Value::String("1".into()));
    // 1:1 profile is a single nested object, not an array.
    assert!(ada["profiles"].is_object(), "profile embeds as object");
    assert_eq!(ada["profiles"]["bio"], Value::String("hi".into()));
    // Bounded 1:N addresses embed as an array (Ada has two).
    let addrs = ada["addresses"].as_array().expect("addresses array");
    assert_eq!(addrs.len(), 2);
}

#[test]
fn order_doc_embeds_items_with_product_snapshot_in_minor_units() {
    let (dump, plan) = build_plan();
    let orders = docs_by_id(&dump, &plan, "orders");
    let (_id, o1000) = orders
        .iter()
        .find(|(id, _)| id == "1000")
        .expect("order 1000");

    let items = o1000["order_items"].as_array().expect("items array");
    assert_eq!(items.len(), 2); // items 1 and 2 belong to order 1000
    let widget = items
        .iter()
        .find(|it| it["product_id"] == 50)
        .expect("widget line");
    assert_eq!(widget["qty"], Value::from(2));
    // Product snapshot frozen into the line; price is exact minor units.
    assert_eq!(widget["products"]["name"], Value::String("Widget".into()));
    assert_eq!(widget["products"]["price"], Value::from(999));
}

#[test]
fn product_doc_absorbs_join_table_as_id_list() {
    let (dump, plan) = build_plan();
    let products = docs_by_id(&dump, &plan, "products");
    let (_id, widget) = products
        .iter()
        .find(|(id, _)| id == "50")
        .expect("product 50");

    let tag_ids = widget["tag_ids"].as_array().expect("tag_ids array");
    let mut ids: Vec<String> = tag_ids
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["1".to_string(), "2".to_string()]);
}

#[test]
fn id_strategies_follow_primary_keys() {
    let (_dump, plan) = build_plan();
    // Single scalar PK -> _id from that column.
    assert_eq!(
        plan.collection("orders").unwrap().id_strategy,
        IdStrategy::PrimaryKey("id".to_string())
    );
}

#[test]
fn indexes_cover_fk_and_unique_constraints() {
    let (_dump, plan) = build_plan();
    let users = plan.collection("users").unwrap();
    // Unique email preserved as a unique index.
    assert!(users
        .indexes
        .iter()
        .any(|i| i.fields == vec!["email".to_string()] && i.unique));

    let orders = plan.collection("orders").unwrap();
    // FK to users indexed on the owning collection.
    assert!(orders
        .indexes
        .iter()
        .any(|i| i.fields == vec!["user_id".to_string()] && !i.unique));
}

#[test]
fn dropped_report_accounts_for_dropped_constraints() {
    let (_dump, plan) = build_plan();
    let d = &plan.dropped;

    // Every foreign key is reported as no-longer-enforced (7 across the schema).
    assert_eq!(d.foreign_keys, 7);
    // Unique constraints on surviving collections are recreated, not lost.
    assert!(d
        .preserved_unique
        .iter()
        .any(|(t, c)| t == "users" && c == &vec!["email".to_string()]));
    assert!(d
        .preserved_unique
        .iter()
        .any(|(t, c)| t == "products" && c == &vec!["sku".to_string()]));
    // NOT NULL is only reported for columns that survive as queryable fields.
    assert!(d.not_null.iter().any(|(t, c)| t == "users" && c == "email"));
    assert!(d.not_null.iter().all(|(t, _)| plan.collection(t).is_some()));
}
