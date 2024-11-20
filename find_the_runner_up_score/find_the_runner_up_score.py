if __name__ == '__main__':
    n = int(input())
    arr = map(int, input().split())

    runner_up_Score = list(set(arr))
    runner_up_Score.sort(reverse=True)

    print(runner_up_Score[1])