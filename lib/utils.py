def replace_string(string, sep=''):
    '''
        Return the input string without any special character and numbers.
    '''
    if sep=='':
        new_string = sep.join([char.upper() for char in string if char.isalnum()])
        new_string = sep.join([char.upper() for char in new_string if not char.isdigit()])
    elif sep==' ':
        new_string = []
        string_lst = string.split(sep)
        for s in string_lst:
            new_string.append(''.join([char.upper() for char in s if char.isalnum()]))
        new_string = sep.join(new_string)
    return new_string

def find_root(index, ptr):
    dummy = index
    while ptr[dummy]>=0:
        dummy = ptr[dummy]
    return dummy