import sys
sys.path.append('proto')
from proto import common_pb2, sandbox_pb2, sandbox_pb2_grpc, operations_pb2
from grpcConnection import conn
from service import sub


user = conn()


def new_account():
    account_stub = sandbox_pb2_grpc.SandboxServiceStub(user.channel)
    acc = account_stub.OpenSandboxAccount(sandbox_pb2.OpenSandboxAccountRequest(), metadata=user.token)
    print(acc)


def add_money():
    account_stub = sandbox_pb2_grpc.SandboxServiceStub(user.channel)
    price = 500000.0
    kwargs = {
        'currency': 'RUB',
        'units': sub.nano_price(price, 'u'),
        'nano': sub.nano_price(price, 'n'),
    }
    value = common_pb2.MoneyValue(**kwargs)
    money = account_stub.SandboxPayIn(sandbox_pb2.SandboxPayInRequest(account_id=user.account, amount=value),
                                      metadata=user.token)
    print(money)


def get_portfolio():
    info = user.operation().GetPortfolio(operations_pb2.PortfolioRequest(account_id=user.account), metadata=user.token)
    print("\nсодержимое портфеля:")
    for item in info.positions:
        if item.figi != 'RUB000UTSTOM' and item.figi != 'BBG0013HGFT4':
            print(str(item.figi)+" "+str(item.quantity))


if __name__ == "__main__":
    # new_account() # создать новый аккаунт
    # add_money() # добавить денег
    get_portfolio()
