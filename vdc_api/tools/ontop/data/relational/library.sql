BEGIN TRANSACTION;
CREATE TABLE authors(
author_id INTEGER primary key not null,
name text,
country text);
INSERT INTO authors VALUES(1,'George Orwell','United Kingdom');
INSERT INTO authors VALUES(2,'Jane Austen','United Kingdom');
INSERT INTO authors VALUES(3,'Haruki Murakami','Japan');
CREATE TABLE books (
book_id INTEGER PRIMARY KEY NOT NULL,
title TEXT,
publication_year INTEGER,
author_id INTEGER,
FOREIGN KEY (author_id) REFERENCES authors(author_id)
);
INSERT INTO books VALUES(101,'1984',1949,1);
INSERT INTO books VALUES(102,'Pride and Prejudice',1813,2);
INSERT INTO books VALUES(103,'Kafka on the Shore',2002,3);
CREATE TABLE borrowers (
borrower_id INTEGER PRIMARY KEY NOT NULL,
name text,
borrowed_book_id integer,
borrow_date date,
FOREIGN KEY (borrowed_book_id) REFERENCES books(book_id)
);
INSERT INTO borrowers VALUES(5001,'Alice Martin',101,'2024-01-12');
INSERT INTO borrowers VALUES(5002,'Luca Rossi',103,'2024-02-03');
INSERT INTO borrowers VALUES(5003,'Emma Dubois',102,'2024-02-15');
COMMIT;
