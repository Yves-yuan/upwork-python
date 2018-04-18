from cassandra.cluster import Cluster


class UserRows:
    def __init__(self, column_si: int):
        self.data = []
        self.size = 0
        self.read_index = 0
        for i in range(0, column_si):
            self.data.append([])
        self.column_size = column_si

    def append(self, *values):
        index = 0
        for v in values:
            self.data[index].append(v)
            index += 1
        self.size += 1

    def add_column(self):
        self.data.append([])
        self.column_size += 1

    def print_data(self):
        length = len(self.data)
        index = 0
        while index < self.size:
            for i in range(0, length):
                print(self.data[i][index], end=' ')
            print("")
            index += 1

    def read_row(self):
        if self.read_index < self.size:
            res = []
            for i in range(0, self.column_size):
                res.append(self.data[i][self.read_index])
            self.read_index += 1
            return res
        else:
            return None


cluster = Cluster()
session = cluster.connect()
session.set_keyspace('makemoney')
rows = session.execute('select * from singersong')
user_row = UserRows(2)
user_row.add_column()
print("Print original ResultSet data")
for row in rows:
    print(row.song, row.singer)
    user_row.append(row.song, row.singer, "value")
print("\nPrint data of userRow")
user_row.print_data()

print("\nRead row from userRow")
d = user_row.read_row()
while d is not None:
    print(d)
    d = user_row.read_row()
