N = 50000*3+7  # SIZE
row = "1,1\n"

with open("static/bigdata.csv", "w") as file:
    for i in range(N):
        file.write(row)
