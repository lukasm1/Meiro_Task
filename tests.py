from main import OrdersService


orders_service = OrdersService()
print(orders_service.load_data_and_save_to_db("data.ndjson"))
print(orders_service.get_users_who_bought_most_products(3))
print(orders_service.get_orders_created_from_to(1537488666, 1539272684))
