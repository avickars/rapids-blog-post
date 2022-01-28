import os
from cuml import make_regression
import cudf

NUM_START_SAMPLES = 2500

NUM_EXECUTIONS_PER_TEST = 3

NUM_DSIZE_DOUBLINGS = 10

RANDOM_STATE = 23

NUM_FEATURES = 399 + 1


def main():
    try:
        os.mkdir('ml_data')
    except FileExistsError:
        print('dir exists')

    for i in range(0, NUM_DSIZE_DOUBLINGS):
        print('Test:', i)
        if i == 0:
            n_samples = NUM_START_SAMPLES
        else:
            n_samples = n_samples * 2

        # Creating the data
        X, y = make_regression(n_samples=n_samples, n_features=NUM_FEATURES, random_state=RANDOM_STATE)


        try:
            os.mkdir(f"ml_data/{i}")
        except FileExistsError:
            print('dir exists')

        cudf.DataFrame(X).to_csv(f"ml_data/{i}/X.csv", index=False)

        del X
        del y



if __name__ == '__main__':
    main()
