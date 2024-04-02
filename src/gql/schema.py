import strawberry
from .stockQuery import StockQuery

@strawberry.type
class Query(StockQuery):
    pass

# @strawberry.type
# class Mutation():
#     pass

# @strawberry.type
# class Subscription(Subscription):
#     pass

schema = strawberry.Schema(query=Query)
