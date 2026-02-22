"""Top 300 US stocks by market cap (S&P 500 based, as of early 2026)."""

TOP_300_TICKERS: list[str] = [
    # Mega-cap tech
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOG", "GOOGL", "META", "TSLA", "AVGO", "BRK B",
    # Top 20
    "LLY", "JPM", "V", "UNH", "XOM", "MA", "COST", "HD", "PG", "JNJ",
    # 21-50
    "ABBV", "NFLX", "BAC", "CRM", "CVX", "MRK", "KO", "WMT", "AMD", "PEP",
    "TMO", "CSCO", "ADBE", "ACN", "LIN", "MCD", "ABT", "WFC", "PM", "DHR",
    # 51-100
    "TXN", "NOW", "QCOM", "ISRG", "INTU", "CMCSA", "GE", "AMGN", "VZ", "RTX",
    "NEE", "AMAT", "PFE", "UNP", "T", "BKNG", "LOW", "SPGI", "HON", "BLK",
    "GS", "IBM", "DE", "BA", "CAT", "UPS", "SCHW", "AXP", "MDLZ", "PLD",
    "LRCX", "ADI", "MMC", "MS", "GILD", "SYK", "CB", "VRTX", "SBUX", "BMY",
    "ELV", "AMT", "PANW", "CI", "ANET", "MO", "KLAC", "BDX", "FI", "SO",
    # 101-150
    "DUK", "CME", "SNPS", "CDNS", "SHW", "ZTS", "ICE", "BSX", "CL", "EOG",
    "PYPL", "REGN", "MCK", "NOC", "APD", "ITW", "TGT", "CMG", "SLB", "EQIX",
    "MPC", "PH", "ORLY", "HCA", "CSX", "ABNB", "FCX", "MAR", "PNC", "GD",
    "USB", "WELL", "EMR", "PSA", "AJG", "MCO", "AON", "NSC", "CCI", "TDG",
    "GM", "ROP", "CARR", "CTAS", "SRE", "OXY", "PCAR", "NEM", "AEP", "D",
    # 151-200
    "TFC", "AIG", "MET", "AFL", "PRU", "AZO", "F", "KMB", "HUM", "SPG",
    "ALL", "MCHP", "PSX", "MSCI", "FIS", "ECL", "DVN", "ROST", "LHX", "TEL",
    "O", "FTNT", "DHI", "COR", "HLT", "DLR", "BK", "MNST", "A", "PAYX",
    "KHC", "GIS", "AMP", "IQV", "FAST", "CTVA", "IDXX", "GEHC", "ODFL", "YUM",
    "EW", "EA", "VRSK", "CTSH", "XEL", "ED", "EXC", "WEC", "KDP", "MLM",
    # 201-250
    "WTW", "ON", "DXCM", "RMD", "VMC", "CBRE", "ANSS", "STZ", "DD", "GWW",
    "HAL", "NUE", "PPG", "KEYS", "AWK", "BKR", "TSCO", "DOW", "CDW", "FANG",
    "DG", "WBD", "MPWR", "MTD", "BIIB", "ROK", "EFX", "STE", "HPQ", "CHD",
    "BR", "WAB", "CPAY", "EXR", "TRGP", "AVB", "VLTO", "CAH", "SBAC", "FTV",
    "WY", "GLW", "VICI", "FITB", "HUBB", "ES", "RJF", "TYL", "IRM", "HBAN",
    # 251-300
    "PTC", "DTE", "DECK", "ETR", "PPL", "PKG", "CINF", "MTB", "CBOE", "LYB",
    "NTAP", "STT", "TSN", "CLX", "IP", "BALL", "AMCR", "SYY", "EQT", "FE",
    "COO", "MOH", "HOLX", "K", "CF", "MKC", "ZBRA", "TER", "WRB", "RF",
    "SWK", "IEX", "ARE", "DRI", "EXPD", "MAA", "WAT", "ESS", "TRMB", "ALGN",
    "NTRS", "CFG", "AKAM", "DOV", "POOL", "LUV", "RVTY", "JBHT", "BAX", "TXT",
    "HPE", "LDOS", "SWKS", "KMI", "CTRA", "J", "KEY", "LKQ", "GPC", "NDAQ",
]

assert len(TOP_300_TICKERS) == 300
