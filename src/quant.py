def calc_growth(self, dataframe, using="simple"):
    "Return Simple or Compound Growth Rate"
    if using == "compound":
        try:
            avg = (((dataframe.iloc[-1] / dataframe.iloc[0])
                    ** (1 / (dataframe.shape[0] - 1))) - 1)
        except ZeroDivisionError:
            avg = dataframe.pct_change(axis=0)
    else:
        avg = dataframe.pct_change(axis=0)

    avg = self.replace_null_values_with_zero(avg)
    avg = self.correct_columns(avg, prefix="GROWTH_")
    return avg

def combine_files(self, folder, ext="csv", header=None,
                      axis=0, file_col="Symbol", ignore_index=False):
        "Returns dataframe of csv files in folder"
        file_col = file_col.upper()
        file_list = list_filetype(in_folder=folder, extension=ext)

        dataframe_list = []
        try:
            for afile in file_list:
                try:
                    dataframe = pandas.read_csv(
                        f"{folder}{afile}", header=header, index_col=0,
                        squeeze=True
                    )
                    dataframe[f"{file_col}"] = afile[:-4]
                    dataframe_list.append(dataframe)
                except FileNotFoundError:
                    pass
        except TypeError:
            pass

        combined = None
        try:
            combined = concat(
                dataframe_list, axis=axis,
                ignore_index=ignore_index, sort=False
            )
        except ValueError:
            pass

        try:
            return combined.T.set_index(file_col)
        except (KeyError, AttributeError):
            return combined


def price_ratios(self):
        return {
            "RATIO_CASH_PRICE" : "PER_SHARE_NET_CASH",
            "RATIO_DCF_HISTORIC_PRICE" : "PER_SHARE_DCF_HISTORIC",
            "RATIO_DCF_SHY_HISTORIC_PRICE" : "PER_SHARE_DCF_SHY_HISTORIC",
            "RATIO_DCF_FORWARD_PRICE" : "PER_SHARE_DCF_FORWARD",
            "RATIO_DCF_SHY_FORWARD_PRICE" : "PER_SHARE_DCF_SHY_FORWARD",
            "RATIO_EQUITY_PRICE" : "PER_SHARE_NET_EQUITY",
            "RATIO_FCF_PRICE" : "PER_SHARE_NET_FCF",
            "RATIO_FCF_SHY_PRICE" : "PER_SHARE_NET_FCF_SHY",
            "RATIO_INCOME_PRICE" : "PER_SHARE_NET_INCOME",
            "RATIO_TANGIBLE_PRICE" : "PER_SHARE_NET_TANGIBLE",
        }

def calc_price_ratios(self, dataframe):
    for (ratio, numerator) in self.price_ratios().items():
        try:
            dataframe[ratio] = dataframe[numerator] / dataframe["PER_SHARE_MARKET"]
        except ZeroDivisionError:
            dataframe[ratio] = 0.0

def yahoo_finance_download(self, tickers=None, start=None):
        if tickers is not None and start is not None:
            return yahoo_finance.download(
                list(tickers),
                start=start,
                end=datetime.date.today(),
                as_panel=False,
                threading=16
            )

def get_current_prices(self, symbols="", dataframe=None):
    """Return """
    try:
        tickers = dataframe.index
    except AttributeError:
        tickers = symbols

    today = datetime.date.today()
    prices_today = f"prices/{str(today)}_current_prices.pkl"
    try:
        current_prices = pandas.read_pickle(prices_today)
    except FileNotFoundError:
        try:
            prices = self.yahoo_finance_download(
                tickers=tickers, start=today,
            )
        except ValueError:
            prices = self.yahoo_finance_download(
                tickers=tickers,
                start=today - datetime.timedelta(days=1),
            )
        current_prices = prices["Adj Close"].T.squeeze()
        current_prices.to_pickle(prices_today)
    except Exception:
        self.logger.error("I could not download current prices, "
                "using last known prices instead")
        last_known_prices = sorted(
            list_filetype(in_folder="prices", extension="pkl")
        )[-1]
        return pandas.read_pickle(last_known_prices)

    return current_prices

def get_ticker_and_filename(self, filename=None, ticker=None,
                            folder=None):
    if filename is not None:
        ticker = os.path.split(filename)[1].split(".")[0]
        filename = filename
        folder = os.path.dirname(filename)
    elif folder is not None:
        ticker = ticker
        filename = f"{folder}{ticker}.csv"
    return (ticker, filename)

def judge_growth(self, rate):
    try:
        if rate > 0: return 'grew'
        if rate == 0: return 'idled'
        if rate < 0: return 'shrank'
    except TypeError:
        return 'ignore'

def relative_scaler(self, series):
    return (series / (series.max() - series.min()))

def score(self, safety=4, growth=2, price=1, dataframe=None):
    safety_score = dataframe.SCORE_SAFETY.copy()
    returns_score = dataframe.SCORE_RETURNS.copy()
    growth_score = dataframe.SCORE_GROWTH.copy()
    price_score = dataframe.SCORE_PRICE.copy()

    # reward
    safety_score[safety_score > 0] = safety
    growth_score[growth_score > 0] = growth
    price_score[price_score > 0] = price
    #penalty
    safety_score[safety_score <= 0] = 0
    growth_score[growth_score <= 0] = 0
    price_score[price_score <= 0] = 0

    score = sum(
        [safety_score, returns_score, growth_score, price_score]
    )
    return score / max(score)

def score_price_ratios(self, dataframe):
    price_ratios = pandas.np.mean([
        dataframe.RATIO_CASH_PRICE,
        dataframe.RATIO_DCF_SHY_FORWARD_PRICE,
        dataframe.RATIO_DCF_SHY_HISTORIC_PRICE,
        dataframe.RATIO_EQUITY_PRICE,
        dataframe.RATIO_INCOME_PRICE,
        dataframe.RATIO_TANGIBLE_PRICE,
    ], axis=0)

    return self.transform(price_ratios)

def sort_index(self, dataframe):
    try:
        return dataframe.sort_index()
    except TypeError:
        return dataframe

def transform_dict(self, dictionary):
    try:
        result = pandas.DataFrame(dictionary)
    except ValueError:
        result = pandas.Series(dictionary)
    except pandas.core.indexes.base.InvalidIndexError:
        result = pandas.DataFrame.from_dict(
            dictionary, orient='index', dtype=pandas.np.float64
        ).T.drop_duplicates(keep='last')
    return sort_index(result)