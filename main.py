import sqlalchemy.exc
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import json
import operator
import itertools

app = Flask(__name__)

# Connect to Database
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///orders.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = '8BYkEfBA6O6donzWlSihBXox7C0sKR6b'
db = SQLAlchemy(app)


# User TABLE Configuration
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(250), nullable=False)
    city = db.Column(db.String(250), nullable=False)
    orders = db.relationship('Order', backref='user')


# order_product TABLE Configuration
order_product = db.Table('order_product',
                         db.Column('order_id', db.Integer, db.ForeignKey('order.id')),
                         db.Column('product_id', db.Integer, db.ForeignKey('product.id'))
                         )


# Order TABLE Configuration
class Order(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    created = db.Column(db.Integer, nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    products = db.relationship('Product', secondary=order_product, backref='orders')


# Product TABLE Configuration
class Product(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(250), nullable=False)
    price = db.Column(db.Integer, nullable=False)


db.create_all()


class OrdersService:

    def __init__(self):
        self.all_users = []
        self.all_orders = []
        self.all_products = []

    def load_data_and_save_to_db(self, file_path: str) -> str:

        with open(file_path) as ndjson:

            # From ndjson file fill in the list of all orders:
            self.all_orders = [json.loads(line) for line in ndjson]

        # Fill in the table Order with the records from the file:
        for order in self.all_orders:
            try:
                new_order = Order(
                    id=order["id"],
                    created=order["created"],
                    user_id=order["user"]["id"],
                )
                db.session.add(new_order)
                db.session.commit()

            except sqlalchemy.exc.IntegrityError:
                # the order with this ID already exists in the database, therefore roll back:
                db.session.rollback()

        # Fill in the list of all users:
        for order in self.all_orders:
            current_user = order["user"]
            if current_user not in self.all_users:
                self.all_users.append(current_user)

        # Fill in the table User with the records from the file:
        for user in self.all_users:
            try:
                new_user = User(
                    id=user["id"],
                    name=user["name"],
                    city=user["city"],
                )
                db.session.add(new_user)
                db.session.commit()

            except sqlalchemy.exc.IntegrityError:
                # the user with this ID already exists in the database, therefore roll back:
                db.session.rollback()

        # Fill in the list of all products:
        for order in self.all_orders:
            current_products = order["products"]
            for product in current_products:
                if product not in self.all_products:
                    self.all_products.append(product)

        # Fill in the table Order with the records from the file:
        for product in self.all_products:
            try:
                new_product = Product(
                    id=product["id"],
                    name=product["name"],
                    price=product["price"],
                )
                db.session.add(new_product)
                db.session.commit()

            except sqlalchemy.exc.IntegrityError:
                # the product with this ID already exists in the database, therefore roll back:
                db.session.rollback()

        return "Database created."

    def get_orders_created_from_to(self, time_period_from: int, time_period_to: int) -> dict:

        # find all orders created within the desired time period:
        desired_orders = Order.query.filter(time_period_from <= Order.created, Order.created <= time_period_to)

        # return a dictionary with orders within the desired time period,
        # where Key = order.id and Value = order.created:
        return {f"Order ID: {order.id}": f"Created: {order.created}" for order in desired_orders}

    def get_users_who_bought_most_products(self, number_of_desired_users: int) -> dict:

        # create a dict with users and their counted orders:
        users_and_counted_orders = {}

        for user in self.all_users:
            current_user_id = user["id"]

            # count the number of orders each user made:
            counted_orders = Order.query.filter(Order.user_id == current_user_id).count()

            # create a dict where Key = user.id and Value = amount of orders they made:
            # Note: It is formatted as a string and that's why the following line of code gets highlighted.
            users_and_counted_orders[f"User ID: {current_user_id}"] = f"Number of orders: {counted_orders}"

        # sort the dict by value, in a descending way, so that we have the highest amounts of orders first:
        users_ids_and_their_orders = dict(
            sorted(users_and_counted_orders.items(), key=operator.itemgetter(1), reverse=True))

        # slice the dictionary to get the desired number of users with the highest number of orders:
        sliced_users_dict = dict(itertools.islice(users_ids_and_their_orders.items(), number_of_desired_users))

        return sliced_users_dict


if __name__ == '__main__':
    app.run(debug=True)
