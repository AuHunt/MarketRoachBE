import strawberry
from src.gql.stock_query import StockQuery

@strawberry.type
class Query(StockQuery):
    '''Extended query inheriting/extending other subqueries'''

# @strawberry.type
# class Mutation():
#     pass

# @strawberry.type
# class Subscription(Subscription):
#     pass

schema = strawberry.Schema(query=Query)
