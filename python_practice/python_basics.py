# 🐍 Python Basics Refresher

# 1. Conditional Logic
#
#Use elif so Python stops once it finds a matching condition:
n = int(input("Enter a number: "))
if n % 2 != 0:
    print("Weird")
elif n % 2 == 0 and 2 <= n <= 5:
    print("Not Weird")
elif n % 2 == 0 and 6 <= n <= 20:
    print("Weird")
else:
    print("Not Weird")

# 2. List Comprehension Example
squares = [i**2 for i in range(1, 11)]
print("Squares:", squares)

# 3. File Writing Example
with open("data_output.txt", "w") as f:
    f.write("This is a test file for Week 1.\n")
