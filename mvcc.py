import utils as u

x, y, z = u.createTransaction()

for el in x:
    print(f'id:{el.id} op:{el.op} res:{el.res}')
print(y)
print(z)

u.prettyPrint(x)
