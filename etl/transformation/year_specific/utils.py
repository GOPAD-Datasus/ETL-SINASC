def _parse_dates(x):
    if type(x) == str:
        area = x[4:6]

        if area == '09':
            return x[:4] + '19' + x[6:]
        elif area == '00':
            return x[:4] + '20' + x[6:]
        else:
            return x
    else:
        return x