from pandas import concat, Series

def zero_numerator(numerator, denominator):
    return -abs(denominator[numerator == 0])

def negative_denominator(numerator, denominator):
    # returns -abs(x/y) when y is negative
    return -abs(numerator[(numerator != 0) & (denominator < 0)] / denominator[(numerator != 0) & (denominator < 0)])

def negative_numerator(numerator, denominator):
    return -abs(numerator[(numerator < 0) & (denominator > 0)] / denominator[(numerator < 0) & (denominator > 0)])

def negative_numerator_zero_denominator(numerator, denominator):
    return -abs(numerator[(numerator < 0) & (denominator == 0)])

def zero_denominator(numerator, denominator):
    return numerator[(numerator > 0) & (denominator == 0)]

def classic_series_ratio(numerator, denominator):
    return numerator[(numerator > 0) & (denominator > 0)] / denominator[(numerator > 0) & (denominator > 0)]

def get_series_ratio(numerator, denominator):
    return concat(
        [
            zero_numerator(numerator, denominator), 
            negative_denominator(numerator, denominator), 
            negative_numerator(numerator, denominator),
            negative_numerator_zero_denominator(numerator, denominator), 
            zero_denominator(numerator, denominator), 
            classic_series_ratio(numerator, denominator)
        ],axis=0
    )

def get_dataframe_ratio(numerator, denominator):
    # returns -abs(y) when x is 0
    if numerator == 0: return -abs(denominator)
    # returns -abs(numerator/y) when y is negative
    if numerator != 0 and denominator < 0: return -abs(numerator / denominator)
    if numerator < 0 and denominator > 0: return -abs(numerator / denominator)
    # Defines Zero Division
    if numerator < 0 and denominator == 0: return -abs(numerator)
    if numerator > 0 and denominator == 0: return numerator
    # regular ratio
    if numerator > 0 and denominator > 0: return numerator / denominator

def get_ratio(numerator, denominator):
    """return ratio for positive numbers
       return diff for denominators less than or equal to zero
    """
    # y = denominator

    if isinstance(numerator, Series) and isinstance(denominator, Series):
        return get_series_ratio(numerator, denominator)
    else:
        return get_dataframe_ratio(numerator, denominator)