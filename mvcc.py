import utils as u

x, y, z = u.createTransaction()
u.prettyPrint(x)

for el in x:
    print(el.pr())
print(y)
print(z)
