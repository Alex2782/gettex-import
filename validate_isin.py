#https://github.com/arthurdejong/python-stdnum/blob/master/stdnum/isin.py

# the letters allowed in an ISIN
_alphabet = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'

def calc_check_digit(number):
    """Calculate the check digits for the number."""
    # convert to numeric first, then double some, then sum individual digits
    number = ''.join(str(_alphabet.index(n)) for n in number)
    number = ''.join(
        str((2, 1)[i % 2] * int(n)) for i, n in enumerate(reversed(number)))
    return str((10 - sum(int(n) for n in number)) % 10)


def validate_isin(isin):

    valid = True
    if len(isin) != 12: valid = False
    elif calc_check_digit(isin[:-1]) != isin[-1]: valid = False

    return valid



