import utils as u

x, y = u.createTransaction()

for el in x:
    print(f'id:{el.id} op:{el.op} res:{el.res}')
print(y)

u.prettyPrint(x)
