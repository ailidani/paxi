
import sys
import numpy as np

data = np.loadtxt(sys.argv[1])
print(np.average(data))
print(np.median(data))
print(np.min(data))
print(np.max(data))
print(np.percentile(data, 95))
print(np.percentile(data, 99))
print(np.percentile(data, 99.9))

