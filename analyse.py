import pandas as pd

import matplotlib as mpl
# We will not be showing images because we don't haw UI
mpl.use('Agg')
import matplotlib.pyplot as plt

# Read data
df = pd.read_csv('df.csv')

# Aggregate by the same column twice
result = df.groupby('site').agg({'size': 'sum', 'site': 'count'})

# We can also use lambdas
# result = df.groupby('site').agg({'size': lambda x: x.sum(), 'site': lambda x: x.count()})

result.columns.values[0] = 'number_of_sites'
result.columns.values[1] = 'sum_size'

# Print results
print(result)

# Construct plot
result.plot(kind='bar', subplots=True, layout=(2,1), figsize=(8, 6), fontsize=5)

# Remove xlabel
plt.xlabel(' ')

# We have a lot of data so turn on tight layout mode
plt.tight_layout()

# Export plot to file
plt.savefig('figure.pdf', dpi=100)
