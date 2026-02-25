use seedkit_core::schema::types::*;

/// Create a simple ecommerce schema for testing.
pub fn ecommerce_schema() -> DatabaseSchema {
    let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "test_ecommerce".to_string());

    // users table
    let mut users = Table::new("users".to_string());
    let mut id = Column::new("id".to_string(), DataType::Serial, "serial".to_string());
    id.is_auto_increment = true;
    id.nullable = false;
    users.columns.insert("id".to_string(), id);

    let mut email = Column::new(
        "email".to_string(),
        DataType::VarChar,
        "character varying".to_string(),
    );
    email.nullable = false;
    email.max_length = Some(255);
    users.columns.insert("email".to_string(), email);

    let mut first_name = Column::new(
        "first_name".to_string(),
        DataType::VarChar,
        "character varying".to_string(),
    );
    first_name.max_length = Some(100);
    users.columns.insert("first_name".to_string(), first_name);

    let mut last_name = Column::new(
        "last_name".to_string(),
        DataType::VarChar,
        "character varying".to_string(),
    );
    last_name.max_length = Some(100);
    users.columns.insert("last_name".to_string(), last_name);

    let password_hash = Column::new(
        "password_hash".to_string(),
        DataType::VarChar,
        "character varying".to_string(),
    );
    users
        .columns
        .insert("password_hash".to_string(), password_hash);

    let mut is_active = Column::new(
        "is_active".to_string(),
        DataType::Boolean,
        "boolean".to_string(),
    );
    is_active.has_default = true;
    users.columns.insert("is_active".to_string(), is_active);

    let created_at = Column::new(
        "created_at".to_string(),
        DataType::TimestampTz,
        "timestamp with time zone".to_string(),
    );
    users.columns.insert("created_at".to_string(), created_at);

    let updated_at = Column::new(
        "updated_at".to_string(),
        DataType::TimestampTz,
        "timestamp with time zone".to_string(),
    );
    users.columns.insert("updated_at".to_string(), updated_at);

    users.primary_key = Some(PrimaryKey {
        columns: vec!["id".to_string()],
        name: Some("users_pkey".to_string()),
    });
    users.unique_constraints.push(UniqueConstraint {
        name: Some("users_email_key".to_string()),
        columns: vec!["email".to_string()],
    });

    schema.tables.insert("users".to_string(), users);

    // categories table
    let mut categories = Table::new("categories".to_string());
    let mut cat_id = Column::new("id".to_string(), DataType::Serial, "serial".to_string());
    cat_id.is_auto_increment = true;
    cat_id.nullable = false;
    categories.columns.insert("id".to_string(), cat_id);

    let mut cat_name = Column::new(
        "name".to_string(),
        DataType::VarChar,
        "character varying".to_string(),
    );
    cat_name.nullable = false;
    categories.columns.insert("name".to_string(), cat_name);

    let slug = Column::new(
        "slug".to_string(),
        DataType::VarChar,
        "character varying".to_string(),
    );
    categories.columns.insert("slug".to_string(), slug);

    let mut parent_id = Column::new(
        "parent_id".to_string(),
        DataType::Integer,
        "integer".to_string(),
    );
    parent_id.nullable = true;
    categories
        .columns
        .insert("parent_id".to_string(), parent_id);

    categories.primary_key = Some(PrimaryKey {
        columns: vec!["id".to_string()],
        name: Some("categories_pkey".to_string()),
    });
    categories.foreign_keys.push(ForeignKey {
        name: Some("categories_parent_id_fkey".to_string()),
        source_columns: vec!["parent_id".to_string()],
        referenced_table: "categories".to_string(),
        referenced_columns: vec!["id".to_string()],
        on_delete: ForeignKeyAction::SetNull,
        on_update: ForeignKeyAction::NoAction,
        is_deferrable: false,
    });

    schema.tables.insert("categories".to_string(), categories);

    // products table
    let mut products = Table::new("products".to_string());
    let mut prod_id = Column::new("id".to_string(), DataType::Serial, "serial".to_string());
    prod_id.is_auto_increment = true;
    prod_id.nullable = false;
    products.columns.insert("id".to_string(), prod_id);

    let mut prod_name = Column::new(
        "name".to_string(),
        DataType::VarChar,
        "character varying".to_string(),
    );
    prod_name.nullable = false;
    products.columns.insert("name".to_string(), prod_name);

    let description = Column::new(
        "description".to_string(),
        DataType::Text,
        "text".to_string(),
    );
    products
        .columns
        .insert("description".to_string(), description);

    let mut price = Column::new(
        "price".to_string(),
        DataType::Numeric,
        "numeric".to_string(),
    );
    price.nullable = false;
    products.columns.insert("price".to_string(), price);

    let mut prod_cat_id = Column::new(
        "category_id".to_string(),
        DataType::Integer,
        "integer".to_string(),
    );
    prod_cat_id.nullable = true;
    products
        .columns
        .insert("category_id".to_string(), prod_cat_id);

    let sku = Column::new(
        "sku".to_string(),
        DataType::VarChar,
        "character varying".to_string(),
    );
    products.columns.insert("sku".to_string(), sku);

    let image_url = Column::new(
        "image_url".to_string(),
        DataType::VarChar,
        "character varying".to_string(),
    );
    products.columns.insert("image_url".to_string(), image_url);

    products.primary_key = Some(PrimaryKey {
        columns: vec!["id".to_string()],
        name: Some("products_pkey".to_string()),
    });
    products.foreign_keys.push(ForeignKey {
        name: Some("products_category_id_fkey".to_string()),
        source_columns: vec!["category_id".to_string()],
        referenced_table: "categories".to_string(),
        referenced_columns: vec!["id".to_string()],
        on_delete: ForeignKeyAction::SetNull,
        on_update: ForeignKeyAction::NoAction,
        is_deferrable: false,
    });
    products.unique_constraints.push(UniqueConstraint {
        name: Some("products_sku_key".to_string()),
        columns: vec!["sku".to_string()],
    });
    products.check_constraints.push(CheckConstraint {
        name: Some("products_price_check".to_string()),
        expression: "(price >= 0)".to_string(),
        parsed: Some(ParsedCheck::GreaterThanOrEqual {
            column: "price".to_string(),
            value: 0.0,
        }),
    });

    schema.tables.insert("products".to_string(), products);

    // orders table
    let mut orders = Table::new("orders".to_string());
    let mut ord_id = Column::new("id".to_string(), DataType::Serial, "serial".to_string());
    ord_id.is_auto_increment = true;
    ord_id.nullable = false;
    orders.columns.insert("id".to_string(), ord_id);

    let mut ord_user_id = Column::new(
        "user_id".to_string(),
        DataType::Integer,
        "integer".to_string(),
    );
    ord_user_id.nullable = false;
    orders.columns.insert("user_id".to_string(), ord_user_id);

    let mut status = Column::new(
        "status".to_string(),
        DataType::VarChar,
        "character varying".to_string(),
    );
    status.nullable = false;
    orders.columns.insert("status".to_string(), status);

    let total = Column::new(
        "total".to_string(),
        DataType::Numeric,
        "numeric".to_string(),
    );
    orders.columns.insert("total".to_string(), total);

    let order_number = Column::new(
        "order_number".to_string(),
        DataType::VarChar,
        "character varying".to_string(),
    );
    orders
        .columns
        .insert("order_number".to_string(), order_number);

    let created_at = Column::new(
        "created_at".to_string(),
        DataType::TimestampTz,
        "timestamp with time zone".to_string(),
    );
    orders.columns.insert("created_at".to_string(), created_at);

    orders.primary_key = Some(PrimaryKey {
        columns: vec!["id".to_string()],
        name: Some("orders_pkey".to_string()),
    });
    orders.foreign_keys.push(ForeignKey {
        name: Some("orders_user_id_fkey".to_string()),
        source_columns: vec!["user_id".to_string()],
        referenced_table: "users".to_string(),
        referenced_columns: vec!["id".to_string()],
        on_delete: ForeignKeyAction::Cascade,
        on_update: ForeignKeyAction::NoAction,
        is_deferrable: false,
    });

    schema.tables.insert("orders".to_string(), orders);

    // order_items table
    let mut order_items = Table::new("order_items".to_string());
    let mut oi_id = Column::new("id".to_string(), DataType::Serial, "serial".to_string());
    oi_id.is_auto_increment = true;
    oi_id.nullable = false;
    order_items.columns.insert("id".to_string(), oi_id);

    let mut oi_order_id = Column::new(
        "order_id".to_string(),
        DataType::Integer,
        "integer".to_string(),
    );
    oi_order_id.nullable = false;
    order_items
        .columns
        .insert("order_id".to_string(), oi_order_id);

    let mut oi_product_id = Column::new(
        "product_id".to_string(),
        DataType::Integer,
        "integer".to_string(),
    );
    oi_product_id.nullable = false;
    order_items
        .columns
        .insert("product_id".to_string(), oi_product_id);

    let mut quantity = Column::new(
        "quantity".to_string(),
        DataType::Integer,
        "integer".to_string(),
    );
    quantity.nullable = false;
    order_items.columns.insert("quantity".to_string(), quantity);

    let unit_price = Column::new(
        "unit_price".to_string(),
        DataType::Numeric,
        "numeric".to_string(),
    );
    order_items
        .columns
        .insert("unit_price".to_string(), unit_price);

    order_items.primary_key = Some(PrimaryKey {
        columns: vec!["id".to_string()],
        name: Some("order_items_pkey".to_string()),
    });
    order_items.foreign_keys.push(ForeignKey {
        name: Some("order_items_order_id_fkey".to_string()),
        source_columns: vec!["order_id".to_string()],
        referenced_table: "orders".to_string(),
        referenced_columns: vec!["id".to_string()],
        on_delete: ForeignKeyAction::Cascade,
        on_update: ForeignKeyAction::NoAction,
        is_deferrable: false,
    });
    order_items.foreign_keys.push(ForeignKey {
        name: Some("order_items_product_id_fkey".to_string()),
        source_columns: vec!["product_id".to_string()],
        referenced_table: "products".to_string(),
        referenced_columns: vec!["id".to_string()],
        on_delete: ForeignKeyAction::Cascade,
        on_update: ForeignKeyAction::NoAction,
        is_deferrable: false,
    });
    order_items.check_constraints.push(CheckConstraint {
        name: Some("order_items_quantity_check".to_string()),
        expression: "(quantity > 0)".to_string(),
        parsed: Some(ParsedCheck::GreaterThan {
            column: "quantity".to_string(),
            value: 0.0,
        }),
    });

    schema.tables.insert("order_items".to_string(), order_items);

    schema
}

/// Create a schema with circular foreign key dependencies.
pub fn circular_schema() -> DatabaseSchema {
    let mut schema = DatabaseSchema::new(DatabaseType::PostgreSQL, "test_circular".to_string());

    // employees table — manager_id references employees.id (self-referencing)
    let mut employees = Table::new("employees".to_string());
    let mut emp_id = Column::new("id".to_string(), DataType::Serial, "serial".to_string());
    emp_id.is_auto_increment = true;
    emp_id.nullable = false;
    employees.columns.insert("id".to_string(), emp_id);

    let emp_name = Column::new(
        "name".to_string(),
        DataType::VarChar,
        "character varying".to_string(),
    );
    employees.columns.insert("name".to_string(), emp_name);

    let mut manager_id = Column::new(
        "manager_id".to_string(),
        DataType::Integer,
        "integer".to_string(),
    );
    manager_id.nullable = true;
    employees
        .columns
        .insert("manager_id".to_string(), manager_id);

    employees.primary_key = Some(PrimaryKey {
        columns: vec!["id".to_string()],
        name: Some("employees_pkey".to_string()),
    });
    employees.foreign_keys.push(ForeignKey {
        name: Some("employees_manager_id_fkey".to_string()),
        source_columns: vec!["manager_id".to_string()],
        referenced_table: "employees".to_string(),
        referenced_columns: vec!["id".to_string()],
        on_delete: ForeignKeyAction::SetNull,
        on_update: ForeignKeyAction::NoAction,
        is_deferrable: false,
    });

    schema.tables.insert("employees".to_string(), employees);

    // departments table — head_id references employees.id
    let mut departments = Table::new("departments".to_string());
    let mut dept_id = Column::new("id".to_string(), DataType::Serial, "serial".to_string());
    dept_id.is_auto_increment = true;
    dept_id.nullable = false;
    departments.columns.insert("id".to_string(), dept_id);

    let dept_name = Column::new(
        "name".to_string(),
        DataType::VarChar,
        "character varying".to_string(),
    );
    departments.columns.insert("name".to_string(), dept_name);

    let mut head_id = Column::new(
        "head_id".to_string(),
        DataType::Integer,
        "integer".to_string(),
    );
    head_id.nullable = true;
    departments.columns.insert("head_id".to_string(), head_id);

    departments.primary_key = Some(PrimaryKey {
        columns: vec!["id".to_string()],
        name: Some("departments_pkey".to_string()),
    });
    departments.foreign_keys.push(ForeignKey {
        name: Some("departments_head_id_fkey".to_string()),
        source_columns: vec!["head_id".to_string()],
        referenced_table: "employees".to_string(),
        referenced_columns: vec!["id".to_string()],
        on_delete: ForeignKeyAction::SetNull,
        on_update: ForeignKeyAction::NoAction,
        is_deferrable: false,
    });

    schema.tables.insert("departments".to_string(), departments);

    schema
}
