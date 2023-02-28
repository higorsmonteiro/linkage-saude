import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

def score_summary(score_arr, bins, range_certain, range_potential, scale="linear"):
    '''
        Plot a distribution of the scores resulted from the data matching process.

        Args:
        -----
            score_arr:
                np.array or pd.Series. List of scores for each pair compared during
                the matching process. When a pandas Series is parsed, the index is 
                expected to be a MultiIndex containing the IDs of the records compared.
            bins:
                list. Custom histogram bins.
            range_certain:
                list. List of size two containing the lower and upper bound of the score
                range of the records considered to be matched for certain.
            range_potential:
                list. List of size two containing the lower and upper bound of the score
                range of the records considered to be potential matching.
            scale:
                String. {'linear', 'log', ...}. Scale of the y-axis.
    '''
    fig, ax = plt.subplots(1, figsize=(7,4.8))
    s = sns.histplot(score_arr, bins=bins, color="tab:red", ax=ax, alpha=0.65)

    ax.spines['top'].set_color('none')
    ax.spines['right'].set_color('none')
    ax.spines['left'].set_position(('outward', 7))
    ax.spines['bottom'].set_position(('outward', 7))

    ax.spines["left"].set_linewidth(1.5)
    ax.spines["bottom"].set_linewidth(1.5)
    ax.spines["top"].set_linewidth(1.5)
    ax.spines["right"].set_linewidth(1.5)

    ax.tick_params(width=1.5, labelsize=11)
    ax.set_ylabel("Frequência", weight="bold", fontsize=14, labelpad=8)
    ax.set_xlabel("Score", weight="bold", fontsize=13)
    ax.grid(alpha=0.2)

    freq, bins = np.histogram(score_arr, bins=bins)
    ax.fill_between(range_potential, y1=max(freq)+10, color="tab:orange", alpha=0.2)
    ax.fill_between(range_certain, y1=max(freq)+10, color="tab:blue", alpha=0.2)

    ncertain = score_arr[(score_arr>=range_certain[0]) & (score_arr<range_certain[1])].shape[0]
    npotential = score_arr[(score_arr>=range_potential[0]) & (score_arr<range_potential[1])].shape[0]
    ndiff = score_arr[(score_arr<range_potential[0])].shape[0]

    ax.set_xticks(bins)
    ax.set_xticklabels([f"{n:.1f}" for n in bins], rotation=45)
    ax.set_yscale(scale)
    return {"FIG": fig, "AXIS": ax, "FREQUENCY AND BINS": (freq, bins), 
            "# IGUAIS": ncertain, "# POTENCIAIS": npotential, "# DIFERENTES": ndiff }