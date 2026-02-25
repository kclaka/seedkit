-- SeedKit Test Fixture: Circular Foreign Keys (MySQL)
-- Tests: cycle detection, edge breaking, deferred FK resolution

CREATE TABLE departments (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    head_id INT,
    budget DECIMAL(12,2) CHECK (budget >= 0)
) ENGINE=InnoDB;

CREATE TABLE employees (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE,
    department_id INT,
    manager_id INT,
    FOREIGN KEY (department_id) REFERENCES departments(id) ON DELETE SET NULL,
    FOREIGN KEY (manager_id) REFERENCES employees(id) ON DELETE SET NULL
) ENGINE=InnoDB;

-- Add circular FK
ALTER TABLE departments
    ADD CONSTRAINT departments_head_id_fkey
    FOREIGN KEY (head_id) REFERENCES employees(id) ON DELETE SET NULL;
