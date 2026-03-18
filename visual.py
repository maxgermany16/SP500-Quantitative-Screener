import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Read the uploaded CSV data
df = pd.read_csv('portfolio_data.csv')

# Set modern, clean style for a portfolio
plt.style.use('dark_background')
fig, ax = plt.subplots(figsize=(10, 6), dpi=300)

# Create a scatter plot: PE Ratio vs Revenue Growth, colored by Sector
scatter = sns.scatterplot(
    data=df, 
    x='pe_ratio', 
    y='rev_growth_pct', 
    hue='sector', 
    s=200, 
    alpha=0.8,
    palette='Set2',
    ax=ax
)

# Add Ticker labels to the points
for i in range(df.shape[0]):
    ax.text(
        df['pe_ratio'].iloc[i] + 0.3, 
        df['rev_growth_pct'].iloc[i], 
        df['ticker'].iloc[i], 
        fontsize=9, 
        color='white',
        weight='bold'
    )

# Formatting
ax.set_title('S&P 500 Screener Output: P/E Ratio vs. Revenue Growth', fontsize=14, weight='bold', pad=15)
ax.set_xlabel('Price to Earnings (P/E) Ratio', fontsize=11, labelpad=10)
ax.set_ylabel('Revenue Growth (%) YoY', fontsize=11, labelpad=10)

# Clean up legend
ax.legend(title='Sector', bbox_to_anchor=(1.05, 1), loc='upper left', frameon=False)
plt.tight_layout()

# Save the plot
plt.savefig('SP500_Screener_Output.jpeg')
print("Plot saved successfully as SP500_Screener_Output.jpeg")

# Display the top 5 for the HTML table
print("\n--- Top 5 Rows for your HTML Table ---")
print(df.head(5).to_markdown())