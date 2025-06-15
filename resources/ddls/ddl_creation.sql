drop table customers;
drop table orders;
drop table shippings;

CREATE TABLE IF NOT EXISTS Categories (
  CategoryID int primary key,
  CategoryName varchar(100) DEFAULT NULL,
  Description varchar(300) DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS Customers (
  CustomerID int primary key,
  CustomerName varchar(100) DEFAULT NULL,
  ContactName varchar(100) DEFAULT NULL,
  Address varchar(300) DEFAULT NULL,
  City varchar(100) DEFAULT NULL,
  PostalCode varchar(20) DEFAULT NULL,
  Country varchar(20) DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS Employees (
  EmployeeID int primary key,
  LastName varchar(50) DEFAULT NULL,
  FirstName varchar(50) DEFAULT NULL,
  BirthDate date DEFAULT NULL,
  Photo varchar(100) DEFAULT NULL,
  Notes varchar(421) DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS Shippers (
  ShipperID int primary key,
  ShipperName varchar(17) DEFAULT NULL,
  Phone varchar(15) DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS Orders (
  OrderID int primary key,
  CustomerID int DEFAULT NULL,
  EmployeeID int DEFAULT NULL,
  OrderDate timestamp DEFAULT current_timestamp,
  ShipperID int DEFAULT NULL,
  foreign key (CustomerID) references Customers(CustomerID),
  foreign key (EmployeeID) references Employees(EmployeeID),
  foreign key (ShipperID) references Shippers(ShipperID)
);

CREATE TABLE IF NOT EXISTS Suppliers (
  SupplierID int primary key,
  SupplierName varchar(100) DEFAULT NULL,
  ContactName varchar(100) DEFAULT NULL,
  Address varchar(300) DEFAULT NULL,
  City varchar(50) DEFAULT NULL,
  PostalCode varchar(20) DEFAULT NULL,
  Country varchar(20) DEFAULT NULL,
  Phone varchar(16) DEFAULT NULL
);


CREATE TABLE IF NOT EXISTS Products (
  ProductID int primary key,
  ProductName varchar(33) DEFAULT NULL,
  SupplierID int DEFAULT NULL,
  CategoryID int DEFAULT NULL,
  Unit varchar(100) DEFAULT NULL,
  Price int DEFAULT NULL,
  foreign key (SupplierID) references Suppliers(SupplierID),
  foreign key (CategoryID) references Categories(CategoryID)
);

CREATE TABLE IF NOT EXISTS OrderDetails (
  OrderDetailID int primary key,
  OrderID int DEFAULT NULL,
  ProductID int DEFAULT NULL,
  Quantity int DEFAULT NULL,
  foreign key (OrderID) references Orders(OrderID),
  foreign key (ProductID) references Products(ProductID)
);
