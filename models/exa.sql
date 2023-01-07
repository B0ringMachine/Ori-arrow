create table order_products_prior_1 as 
(select a.*,b.product_id,b.add_to_cart_order,b.reordered
from orders as a
join order_products_prior as b 
on a.order_id=b.order_id
where a.eval_set='prior');
