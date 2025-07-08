import pandas as pd

input_file = "part-r-00000"
output_file = "part-r-00000-sorted"

df = pd.read_csv(input_file, sep="\t", header=None, names=["Term", "Frequency"])

df_sorted = df.sort_values(by="Frequency", ascending=False)
top_100 = df_sorted.head(100)
top_100.to_csv(output_file, sep="\t", index=False, header=False)

print(f"Top 100 terms saved to {output_file}")
