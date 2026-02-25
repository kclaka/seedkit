-- SeedKit Test Fixture: Circular Foreign Keys
-- Tests: cycle detection, edge breaking, deferred FK resolution

CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE,
    department_id INTEGER, -- FK added after departments table
    manager_id INTEGER REFERENCES employees(id) ON DELETE SET NULL
);

CREATE TABLE departments (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    head_id INTEGER REFERENCES employees(id) ON DELETE SET NULL,
    budget NUMERIC(12,2) CHECK (budget >= 0)
);

-- Add circular FK
ALTER TABLE employees
    ADD CONSTRAINT employees_department_id_fkey
    FOREIGN KEY (department_id) REFERENCES departments(id) ON DELETE SET NULL;
