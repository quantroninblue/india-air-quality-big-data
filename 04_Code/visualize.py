"""Step 5 - Charts: 05_Outputs/tables/*.csv -> 06_Visualisations/*.png (static, print-ready).

Palette: validated categorical slots (blue, orange, aqua, yellow) for identity, one-hue
blue ramp for magnitude / ordinal AQI bands. Thin marks, hairline grid, no dual axes.
"""
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.patches import FancyBboxPatch

from common import FIGURES_DIR, TABLES_DIR

SURFACE, INK, INK2, MUTED = "#fcfcfb", "#0b0b0b", "#52514e", "#898781"
GRID, AXIS = "#e1e0d9", "#c3c2b7"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
BLUE_SEQ = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
AQI_ORDINAL = ["#86b6ef", "#5598e7", "#256abf", "#184f95", "#0d366b"]
AQI_BANDS = ["Good", "Satisfactory", "Moderate", "Poor", "Very Poor / Severe"]
PRETTY = {"pm25": "PM2.5", "pm10": "PM10", "no2": "NO₂", "co": "CO", "so2": "SO₂",
          "o3": "O₃", "no": "NO", "nox": "NOx", "nh3": "NH₃", "benzene": "Benzene",
          "toluene": "Toluene", "at": "Air temp.", "rh": "Humidity", "ws": "Wind speed",
          "wd": "Wind dir.", "rf": "Rainfall", "sr": "Solar rad."}

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "font.family": "DejaVu Sans", "font.size": 10, "text.color": INK,
    "axes.edgecolor": AXIS, "axes.labelcolor": INK2, "axes.titlesize": 12,
    "axes.titleweight": "bold", "axes.titlelocation": "left", "axes.titlepad": 12,
    "xtick.color": MUTED, "ytick.color": MUTED, "xtick.labelcolor": INK2,
    "ytick.labelcolor": INK2, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "axes.spines.top": False, "axes.spines.right": False, "lines.linewidth": 2,
    "lines.solid_capstyle": "round", "legend.frameon": False, "legend.fontsize": 9,
})
MONTHS = list("JFMAMJJASOND")


def t(name):
    return pd.read_csv(TABLES_DIR / f"{name}.csv")


def subtitle(ax, text):
    ax.text(0, 1.01, text, transform=ax.transAxes, fontsize=9, color=INK2, va="bottom")


def save(fig, name):
    fig.tight_layout()
    fig.savefig(FIGURES_DIR / f"{name}.png", dpi=200)
    plt.close(fig)
    print(f"[figure] 06_Visualisations/{name}.png")


def end_label(ax, x, y, text, color=INK2, dx=6, fontsize=9, **kw):
    ax.annotate(text, (x, y), xytext=(dx, 0), textcoords="offset points", va="center",
                fontsize=fontsize, color=color, **kw)


# ---------------------------------------------------------------- overview / 5V
def fig_coverage():
    d = t("coverage_by_year")
    fig, ax = plt.subplots(figsize=(8, 3.8))
    ax.bar(d["year"], d["stations"], width=0.6, color=SERIES[0])
    for x, v in zip(d["year"], d["stations"]):
        ax.text(x, v + 6, f"{v}", ha="center", fontsize=8, color=INK2)
    ax.set_title("Monitoring network grew 15x in 13 years")
    subtitle(ax, "Active CPCB continuous stations per year (2023 = Jan-Mar only)")
    ax.set_xticks(d["year"]); ax.grid(axis="x", visible=False)
    ax.set_ylabel("Stations reporting")
    save(fig, "01_coverage_by_year")


def fig_cleaning():
    d = t("cleaning_log")
    d["affected"] = d["rows_removed"] + d["values_nulled"]
    d = d[d["affected"] > 0].iloc[::-1]
    labels = [r.split(" ", 1)[1] for r in d["rule"]]
    labels = [l[0].upper() + l[1:] for l in labels]
    kinds = ["rows removed" if r > 0 else "values nulled" for r in d["rows_removed"]]
    fig, ax = plt.subplots(figsize=(8.5, 3.8))
    y = np.arange(len(d))
    colors = [SERIES[1] if k == "rows removed" else SERIES[0] for k in kinds]
    ax.barh(y, d["affected"] / 1e6, height=0.55, color=colors)
    for yi, v, k in zip(y, d["affected"], kinds):
        end_label(ax, v / 1e6, yi, f"{v / 1e6:.2f}M {k}")
    ax.set_yticks(y, labels); ax.grid(axis="y", visible=False)
    ax.set_xlim(0, d["affected"].max() / 1e6 * 1.45)
    ax.set_xlabel("Millions")
    ax.set_title("Cleaning: 14.30M raw rows -> 11.96M valid hourly records")
    subtitle(ax, "Rows removed (orange) and individual readings set to null (blue) per rule")
    save(fig, "02_cleaning_rules")


def fig_completeness():
    d = t("completeness").sort_values("pct_available_clean")
    fig, ax = plt.subplots(figsize=(7.5, 4.6))
    y = np.arange(len(d))
    ax.barh(y, d["pct_available_clean"], height=0.6, color=SERIES[0])
    for yi, v in zip(y, d["pct_available_clean"]):
        end_label(ax, v, yi, f"{v:.0f}%")
    ax.set_yticks(y, [PRETTY[m] for m in d["measure"]]); ax.grid(axis="y", visible=False)
    ax.set_xlim(0, 110); ax.set_xlabel("% of hourly records with a valid value")
    ax.set_title("Veracity: no parameter is complete")
    subtitle(ax, "Share of cleaned hourly records carrying each measurement")
    save(fig, "03_completeness")


def fig_benchmark():
    d = t("benchmark_csv_vs_parquet")
    fig, ax = plt.subplots(figsize=(7.5, 2.4))
    y = np.arange(len(d))[::-1]
    ax.barh(y, d["seconds"], height=0.5, color=[SERIES[1], SERIES[0]])
    for yi, v in zip(y, d["seconds"]):
        end_label(ax, v, yi, f"{v:.1f} s")
    ax.set_yticks(y, d["source"]); ax.grid(axis="y", visible=False)
    ax.set_xlim(0, d["seconds"].max() * 1.2); ax.set_xlabel("Seconds (annual mean PM2.5 per station)")
    speedup = d["seconds"].iloc[0] / d["seconds"].iloc[1]
    ax.set_title(f"Same query is {speedup:.0f}x faster on Parquet than raw CSV")
    save(fig, "04_benchmark_csv_vs_parquet")


# ---------------------------------------------------------------- RQ1 spatial
def fig_city_ranking():
    d = t("rq1_city_ranking").dropna(subset=["mean_pm25"])
    top = d.head(15).iloc[::-1]
    fig, ax = plt.subplots(figsize=(8, 5.4))
    y = np.arange(len(top))
    ax.barh(y, top["mean_pm25"], height=0.6, color=SERIES[0])
    for yi, v in zip(y, top["mean_pm25"]):
        end_label(ax, v, yi, f"{v:.0f}")
    ax.axvline(40, color=INK2, lw=1)
    ax.text(41, len(top) - 0.4, "NAAQS annual limit 40", fontsize=8, color=INK2, va="bottom")
    ax.set_yticks(y, [f"{c}, {s}" for c, s in zip(top["city"], top["state"])])
    ax.grid(axis="y", visible=False); ax.set_xlim(0, top["mean_pm25"].max() * 1.15)
    ax.set_xlabel("Mean PM2.5 (µg/m³), 2019-2022")
    ax.set_title("The 15 most polluted cities are all in north India")
    subtitle(ax, "Cities with >= 600 monitored days; every one is 2-2.7x the national standard")
    save(fig, "05_rq1_top_cities")


def fig_region_monthly():
    d = t("rq1_region_monthly")
    fig, ax = plt.subplots(figsize=(8, 4))
    for i, (reg, g) in enumerate(d.groupby("region")):
        ax.plot(g["month"], g["mean_pm25"], color=SERIES[i], marker="o", ms=5,
                mec=SURFACE, mew=1.5, label=reg)
        end_label(ax, 12, g["mean_pm25"].iloc[-1], reg, dx=8)
    ax.axhline(60, color=INK2, lw=1)
    ax.text(1, 62, "24-h NAAQS 60", fontsize=8, color=INK2)
    ax.set_xticks(range(1, 13), MONTHS); ax.set_xlim(0.7, 14.6)
    ax.set_ylabel("Mean PM2.5 (µg/m³)"); ax.legend(loc="upper center", ncol=2)
    ax.set_title("North India's winter PM2.5 is more than double the rest of India")
    subtitle(ax, "Mean daily PM2.5 by month, 2019-2022")
    save(fig, "06_rq1_region_monthly")


def fig_state_categories():
    d = t("rq1_state_aqi_categories")
    d["band"] = d["aqi_category"].replace({"Very Poor": "Very Poor / Severe",
                                           "Severe": "Very Poor / Severe"})
    p = d.pivot_table(index="state", columns="band", values="station_days", aggfunc="sum").fillna(0)
    p = p[p.sum(axis=1) >= 300][AQI_BANDS]
    share = p.div(p.sum(axis=1), axis=0) * 100
    share = share.sort_values(["Very Poor / Severe", "Poor"], ascending=True)
    fig, ax = plt.subplots(figsize=(9, 6.4))
    left = np.zeros(len(share)); y = np.arange(len(share))
    for band, col in zip(AQI_BANDS, AQI_ORDINAL):
        ax.barh(y, share[band], left=left, height=0.62, color=col, label=band,
                edgecolor=SURFACE, linewidth=1)
        left += share[band].values
    bad = share["Poor"] + share["Very Poor / Severe"]
    for yi, v in zip(y, bad):
        end_label(ax, 100, yi, f"{v:.0f}% poor+", dx=4)
    ax.set_yticks(y, share.index); ax.grid(False)
    ax.set_xlim(0, 115); ax.set_xticks([0, 25, 50, 75, 100], ["0", "25", "50", "75", "100%"])
    ax.legend(ncol=5, loc="lower center", bbox_to_anchor=(0.45, -0.13), handlelength=1.2)
    ax.set_title("Share of monitored days by AQI category, 2022")
    subtitle(ax, "States with >= 300 station-days; label = days rated Poor or worse")
    save(fig, "07_rq1_state_aqi_categories")


# ---------------------------------------------------------------- RQ2 temporal
def fig_metro_monthly():
    d = t("rq2_metro_monthly")
    cities = ["Delhi", "Kolkata", "Mumbai", "Hyderabad", "Chennai", "Bengaluru"]
    fig, axes = plt.subplots(2, 3, figsize=(10, 5.2), sharex=True, sharey=True)
    for ax, city in zip(axes.flat, cities):
        for other in cities:
            g = d[d["city"] == other]
            ax.plot(g["month"], g["mean_pm25"], color=GRID, lw=1.2, zorder=1)
        g = d[d["city"] == city]
        ax.plot(g["month"], g["mean_pm25"], color=SERIES[0], zorder=3)
        ax.fill_between(g["month"], g["mean_pm25"], color=SERIES[0], alpha=0.1, zorder=2)
        peak = g.loc[g["mean_pm25"].idxmax()]
        ax.plot(peak["month"], peak["mean_pm25"], "o", color=SERIES[0], ms=6, mec=SURFACE, mew=1.5)
        ax.annotate(f"{peak['mean_pm25']:.0f}", (peak["month"], peak["mean_pm25"]),
                    xytext=(0, 6), textcoords="offset points", ha="center", fontsize=8, color=INK2)
        ax.set_title(city, fontsize=10)
        ax.set_xticks(range(1, 13), MONTHS)
    fig.suptitle("Every metro peaks in winter and bottoms out in the monsoon",
                 x=0.01, ha="left", fontweight="bold", fontsize=12)
    fig.text(0.01, 0.92, "Mean daily PM2.5 (µg/m³) by month, 2019-2022; grey = other metros",
             fontsize=9, color=INK2)
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    fig.savefig(FIGURES_DIR / "08_rq2_metro_monthly.png", dpi=200); plt.close(fig)
    print("[figure] 06_Visualisations/08_rq2_metro_monthly.png")


def fig_diurnal():
    d = t("rq2_diurnal_season")
    order = ["Winter", "Post-monsoon", "Summer", "Monsoon"]
    fig, ax = plt.subplots(figsize=(8, 4))
    for i, s in enumerate(order):
        g = d[d["season"] == s]
        ax.plot(g["hour"], g["mean_pm25"], color=SERIES[i], label=s)
        end_label(ax, 23, g["mean_pm25"].iloc[-1], s)
    ax.set_xticks(range(0, 24, 3), [f"{h:02d}:00" for h in range(0, 24, 3)])
    ax.set_xlim(0, 27); ax.set_ylabel("Mean PM2.5 (µg/m³)")
    ax.legend(loc="upper center", ncol=4)
    ax.set_title("PM2.5 peaks at night and dips mid-afternoon")
    subtitle(ax, "Mean hourly PM2.5 by hour of day (IST) and season, all stations 2019-2022")
    save(fig, "09_rq2_diurnal_season")


def fig_delhi_heatmap():
    d = t("rq2_delhi_month_hour").pivot(index="month", columns="hour", values="mean_pm25")
    fig, ax = plt.subplots(figsize=(9, 4.2))
    cmap = LinearSegmentedColormap.from_list("blue", BLUE_SEQ)
    im = ax.imshow(d.values, aspect="auto", cmap=cmap)
    ax.set_yticks(range(12), ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep",
                              "Oct", "Nov", "Dec"])
    ax.set_xticks(range(0, 24, 2), [f"{h:02d}" for h in range(0, 24, 2)])
    ax.set_xlabel("Hour of day (IST)"); ax.grid(False)
    for s in ax.spines.values():
        s.set_visible(False)
    cb = fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02)
    cb.set_label("Mean PM2.5 (µg/m³)", color=INK2); cb.outline.set_visible(False)
    ax.set_title("Delhi: winter nights are the worst hours of the year")
    subtitle(ax, "Mean PM2.5 by month and hour, 2018-2022, all Delhi stations")
    save(fig, "10_rq2_delhi_heatmap")


def fig_panel_trend():
    d = t("rq2_panel_trend")
    fig, ax = plt.subplots(figsize=(7, 3.6))
    ax.bar(d["year"], d["mean_pm25"], width=0.55, color=SERIES[0])
    for x, v in zip(d["year"], d["mean_pm25"]):
        ax.text(x, v + 1.5, f"{v:.0f}", ha="center", fontsize=9, color=INK2)
    ax.set_xticks(d["year"]); ax.grid(axis="x", visible=False)
    ax.set_ylabel("Mean PM2.5 (µg/m³)")
    n = int(d["stations"].iloc[0])
    ax.set_title("PM2.5 fell 16% from 2018 to 2022 at the same stations")
    subtitle(ax, f"Balanced panel of {n} stations with >= 200 valid days in every year")
    save(fig, "11_rq2_panel_trend")


# ---------------------------------------------------------------- RQ3 Diwali
def fig_diwali_offsets():
    d = t("rq3_diwali_offsets")
    fig, ax = plt.subplots(figsize=(8, 4))
    for i, (area, g) in enumerate(d.groupby("area")):
        ax.plot(g["day_offset"], g["mean_pm25"], color=SERIES[i], marker="o", ms=4,
                mec=SURFACE, mew=1.2, label=area)
    ax.axvline(0, color=INK2, lw=1)
    ax.text(0.2, ax.get_ylim()[1] * 0.97, "Diwali", fontsize=9, color=INK2, va="top")
    peak = d[(d["area"] == "Delhi")].set_index("day_offset")["mean_pm25"]
    ax.annotate(f"{peak[1]:.0f}", (1, peak[1]), xytext=(6, 4), textcoords="offset points",
                fontsize=9, color=INK2)
    ax.set_xlabel("Days relative to Diwali"); ax.set_ylabel("Mean PM2.5 (µg/m³)")
    ax.legend(loc="upper left")
    ax.set_title("PM2.5 jumps the night of Diwali and peaks the day after")
    subtitle(ax, "Mean daily PM2.5 around Diwali, averaged over 2015-2022")
    save(fig, "12_rq3_diwali_offsets")


def fig_diwali_years():
    d = t("rq3_diwali_by_year")
    fig, ax = plt.subplots(figsize=(8, 3.8))
    x = np.arange(len(d)); w = 0.36
    ax.bar(x - w / 2 - 0.01, d["baseline_pm25"], w, color=SERIES[0], label="Baseline (days -10 to -4)")
    ax.bar(x + w / 2 + 0.01, d["day_after_pm25"], w, color=SERIES[1], label="Day after Diwali")
    for xi, r in zip(x, d["ratio"]):
        top = max(d["baseline_pm25"].iloc[xi], d["day_after_pm25"].iloc[xi])
        ax.text(xi, top + 8, f"{r:.1f}x", ha="center", fontsize=8, color=INK2)
    ax.set_xticks(x, d["year"]); ax.grid(axis="x", visible=False)
    ax.set_ylabel("Delhi mean PM2.5 (µg/m³)"); ax.set_ylim(0, d[["baseline_pm25", "day_after_pm25"]].max().max() * 1.18)
    ax.legend(loc="upper left", ncol=2)
    ax.set_title("Delhi: the day after Diwali is up to 4x the pre-festival level")
    subtitle(ax, "Ratio above bars; 2015 and 2020 baselines were already high from crop-residue smoke")
    save(fig, "13_rq3_diwali_by_year")


def fig_diwali_hourly():
    d = t("rq3_delhi_diwali_hourly")
    fig, ax = plt.subplots(figsize=(8, 3.6))
    ax.plot(d["hour_offset"], d["mean_pm25"], color=SERIES[0])
    ax.fill_between(d["hour_offset"], d["mean_pm25"], color=SERIES[0], alpha=0.1)
    pk = d.loc[d["mean_pm25"].idxmax()]
    ax.plot(pk["hour_offset"], pk["mean_pm25"], "o", color=SERIES[0], ms=7, mec=SURFACE, mew=1.5)
    end_label(ax, pk["hour_offset"], pk["mean_pm25"],
              f"{pk['mean_pm25']:.0f} µg/m³ at {int(pk['hour_offset']) % 24:02d}:00")
    ticks = list(range(-24, 73, 12))
    lab = {-24: "Eve 00:00", -12: "12:00", 0: "Diwali 00:00", 12: "12:00", 24: "Day+1 00:00",
           36: "12:00", 48: "Day+2 00:00", 60: "12:00", 72: "Day+3"}
    ax.set_xticks(ticks, [lab[k] for k in ticks], fontsize=8)
    ax.set_ylabel("Delhi mean PM2.5 (µg/m³)")
    ax.set_title("Fireworks drive an overnight spike in Delhi")
    subtitle(ax, "Hourly PM2.5 from the day before Diwali to two days after, mean of 2017-2022")
    save(fig, "14_rq3_diwali_hourly")


# ---------------------------------------------------------------- RQ4 COVID
def fig_lockdown():
    d = t("rq4_lockdown_change").iloc[0]
    pols = ["no2", "pm10", "pm25", "co", "so2", "o3"]
    vals = [d[f"{p}_median_pct_change"] for p in pols]
    fig, ax = plt.subplots(figsize=(7.5, 3.6))
    y = np.arange(len(pols))[::-1]
    ax.barh(y, vals, height=0.55, color=SERIES[0])
    for yi, v in zip(y, vals):
        end_label(ax, v, yi, f"{v:.0f}%", dx=-6, ha="right")
    ax.axvline(0, color=AXIS, lw=1)
    ax.set_yticks(y, [PRETTY[p] for p in pols]); ax.grid(axis="y", visible=False)
    ax.set_xlim(min(vals) * 1.25, 5); ax.set_xlabel("Median % change per station, 2020 vs 2019")
    ax.set_title("Lockdown nearly halved NO₂ and particulates")
    subtitle(ax, f"25 Mar - 31 May 2020 vs same window 2019, {int(d['stations'])} stations in both years")
    save(fig, "15_rq4_lockdown_change")


def fig_lockdown_metros():
    d = t("rq4_lockdown_metros")
    d = d[d["stations"] >= 2]
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.8), sharey=True)
    for ax, (pol, label) in zip(axes, [("pm25", "PM2.5"), ("no2", "NO₂")]):
        y = np.arange(len(d))[::-1]; h = 0.36
        ax.barh(y + h / 2 + 0.01, d[f"{pol}_2019"], h, color=SERIES[0], label="2019")
        ax.barh(y - h / 2 - 0.01, d[f"{pol}_2020"], h, color=SERIES[1], label="2020 lockdown")
        for yi, v in zip(y, d[f"{pol}_2020"]):
            end_label(ax, v, yi - h / 2, f"{v:.0f}", fontsize=8)
        ax.set_yticks(y, d["city"]); ax.grid(axis="y", visible=False)
        ax.set_title(f"{label} (µg/m³)", fontsize=10)
    axes[0].legend(loc="lower right")
    fig.suptitle("Lockdown cut NO₂ by roughly half in Delhi and Bengaluru",
                 x=0.01, ha="left", fontweight="bold", fontsize=12)
    fig.tight_layout()
    fig.savefig(FIGURES_DIR / "16_rq4_lockdown_metros.png", dpi=200); plt.close(fig)
    print("[figure] 06_Visualisations/16_rq4_lockdown_metros.png")


# ---------------------------------------------------------------- RQ5 meteorology
def fig_wind():
    d = t("rq5_wind_bins")
    d = d[d["station_days"] >= 200]
    order = ["Winter", "Post-monsoon", "Summer", "Monsoon"]
    fig, ax = plt.subplots(figsize=(8, 4))
    for i, s in enumerate(order):
        g = d[d["season"] == s]
        ax.plot(g["ws_bin"], g["mean_pm25"], color=SERIES[i], marker="o", ms=5,
                mec=SURFACE, mew=1.5, label=s)
        end_label(ax, g["ws_bin"].iloc[-1], g["mean_pm25"].iloc[-1], s)
    ax.set_xticks(np.arange(0, 4.5, 0.5),
                  [f"{v:.1f}" for v in np.arange(0, 4.0, 0.5)] + ["4+"])
    ax.set_xlim(-0.1, 4.9)
    ax.set_xlabel("Daily mean wind speed (m/s, 0.5 m/s bins)")
    ax.set_ylabel("Mean PM2.5 (µg/m³)"); ax.legend(loc="upper right", ncol=2)
    ax.set_title("Calm days trap pollution, especially in winter")
    subtitle(ax, "Mean daily PM2.5 by wind-speed bin and season, 2019-2022")
    save(fig, "17_rq5_wind_speed")


def fig_architecture():
    top = [
        ("Source", "CPCB CCR via Kaggle\n453 station CSVs\n1.7 GB, 198 layouts"),
        ("HDFS raw", "/cpcb/raw\nhdfs dfs -put\n454 files, 1 block each"),
        ("Ingest (PySpark)", "Group by header\nschema + unit\nharmonisation"),
        ("Bronze", "HDFS Parquet\n14.3M hourly rows\n0.48 GB"),
        ("Clean (PySpark)", "7 rules: dedupe,\nranges, stuck\nsensors, PM check"),
        ("Silver", "HDFS Parquet\nby year, 11.96M rows\n+ station metadata"),
        ("AQI (window fns)", "24-h means,\n8-h rolling max,\nCPCB NAQI"),
        ("Gold", "HDFS Parquet\ndaily station table\nAQI + category"),
    ]
    bottom = {1: ("MapReduce + RDD", "Hadoop Streaming on YARN\nand Spark RDDs read raw\nHDFS files: cross-check"),
              7: ("Hive + Spark SQL", "external / managed tables,\nUDF, 5 research questions\n-> charts, report")}
    fig, ax = plt.subplots(figsize=(12.5, 4.6))
    ax.set_xlim(0, len(top) * 1.5); ax.set_ylim(-2.3, 2.75); ax.axis("off")
    stored = {"Source", "HDFS raw", "Bronze", "Silver", "Gold"}

    def box(x, y, head, body, fc):
        ax.add_patch(FancyBboxPatch((x, y), 1.2, 1.9, boxstyle="round,pad=0,rounding_size=0.08",
                                    fc=fc, ec=SERIES[0], lw=1.2))
        ax.text(x + 0.6, y + 1.65, head, ha="center", va="center", fontsize=9, fontweight="bold")
        ax.text(x + 0.6, y + 0.8, body, ha="center", va="center", fontsize=7.4, color=INK2,
                linespacing=1.4)

    for i, (head, body) in enumerate(top):
        x = i * 1.5 + 0.1
        box(x, 0.25, head, body, "#cde2fb" if head in stored else SURFACE)
        if i < len(top) - 1:
            ax.annotate("", xy=(x + 1.42, 1.2), xytext=(x + 1.22, 1.2),
                        arrowprops=dict(arrowstyle="-|>", color=INK2, lw=1.2))
    for i, (head, body) in bottom.items():
        x = i * 1.5 + 0.1
        box(x - 0.15, -2.2, head, body, SURFACE)
        ax.annotate("", xy=(x + 0.6, -0.28), xytext=(x + 0.6, 0.25),
                    arrowprops=dict(arrowstyle="-|>", color=INK2, lw=1.2))
    ax.text(0.1, 2.5, "Hadoop (HDFS + YARN) and Apache Spark 4.0 pipeline, single-node cluster",
            fontsize=11, fontweight="bold")
    ax.text(len(top) * 1.5 - 0.1, 2.5, "shaded = data stored (local source / HDFS)", fontsize=8,
            color=INK2, ha="right")
    fig.savefig(FIGURES_DIR / "00_architecture.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("[figure] 06_Visualisations/00_architecture.png")


def main():
    for fn in [fig_architecture, fig_coverage, fig_cleaning, fig_completeness, fig_benchmark,
               fig_city_ranking, fig_region_monthly, fig_state_categories, fig_metro_monthly,
               fig_diurnal, fig_delhi_heatmap, fig_panel_trend, fig_diwali_offsets,
               fig_diwali_years, fig_diwali_hourly, fig_lockdown, fig_lockdown_metros, fig_wind]:
        fn()


if __name__ == "__main__":
    main()
