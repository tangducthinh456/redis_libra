#book
select books.name, a.name as author, p.name as publisher, c.name as category from 
	(select * from book where name like '%he%') as books 
    join author a on books.author_id = a.id
    join publisher p on books.publisher_id = p.id
    join category c on books.category_id = c.id;
    
#author
select books.name, p.name as publisher, c.name as category from
(select * from book where author_id in (select id from author where name = 'O. Henry')) as books
	join publisher p on books.publisher_id = p.id
    join category c on books.category_id = c.id;
    
#category
select books.name, a.name as author, p.name as publisher from
(select * from book where category_id in (select id from category where name = 'Fiction')) as books
	join publisher p on books.publisher_id = p.id
    join author a on books.author_id = a.id;
    
#publisher
select books.name, a.name as author, c.name as category from
(select * from book where publisher_id in 
(select id from publisher where name = 'Balboa Press')) as books
	join category c on books.category_id = c.id
    join author a on books.author_id = a.id;