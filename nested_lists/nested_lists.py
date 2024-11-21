if __name__ == "__main__":
    students = []
    for _ in range(int(input())):
        name = input()
        score = float(input())
        students.append([name, score])

    scores = sorted(set([student[1] for student in students]))
    second_score = scores[1]

    order_students = [student[0] for student in students if student[1] == second_score]

    order_students.sort()

    for i in order_students:
        print(i)
