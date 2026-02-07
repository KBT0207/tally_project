import re

CURRENCY_MAP = {
    'USD': {
        'symbols': ['$', 'US$', 'USD'],
        'pattern': r'\$|US\$|USD',
        'names': ['dollar', 'dollars', 'usd']
    },
    'EUR': {
        'symbols': ['€', 'EUR'],
        'pattern': r'€|EUR',
        'names': ['euro', 'euros', 'eur']
    },
    'GBP': {
        'symbols': ['£', 'G£', '\xa3', 'GBP', 'G�'],
        'pattern': r'£|G£|GBP|\xa3',
        'names': ['pound', 'pounds', 'sterling', 'gbp']
    },
    'JPY': {
        'symbols': ['¥', '¥', 'JPY'],
        'pattern': r'¥|JPY',
        'names': ['yen', 'jpy']
    },
    'CNY': {
        'symbols': ['¥', '元', 'CNY', 'RMB'],
        'pattern': r'CNY|RMB|元',
        'names': ['yuan', 'renminbi', 'cny', 'rmb']
    },
    'INR': {
        'symbols': ['₹', '₨', 'Rs', 'Rs.', 'INR'],
        'pattern': r'₹|₨|Rs\.?|INR',
        'names': ['rupee', 'rupees', 'inr']
    },
    'CHF': {
        'symbols': ['CHF', 'Fr', 'SFr'],
        'pattern': r'CHF|SFr\.?',
        'names': ['franc', 'francs', 'chf', 'swiss franc']
    },
    'CAD': {
        'symbols': ['C$', 'CAD', 'CA$'],
        'pattern': r'C\$|CA\$|CAD',
        'names': ['canadian dollar', 'cad']
    },
    'AUD': {
        'symbols': ['A$', 'AUD', 'AU$'],
        'pattern': r'A\$|AU\$|AUD',
        'names': ['australian dollar', 'aud']
    },
    'NZD': {
        'symbols': ['NZ$', 'NZD'],
        'pattern': r'NZ\$|NZD',
        'names': ['new zealand dollar', 'nzd']
    },
    'KRW': {
        'symbols': ['₩', 'KRW'],
        'pattern': r'₩|KRW',
        'names': ['won', 'krw']
    },
    'SGD': {
        'symbols': ['S$', 'SGD'],
        'pattern': r'S\$|SGD',
        'names': ['singapore dollar', 'sgd']
    },
    'HKD': {
        'symbols': ['HK$', 'HKD'],
        'pattern': r'HK\$|HKD',
        'names': ['hong kong dollar', 'hkd']
    },
    'NOK': {
        'symbols': ['kr', 'NOK'],
        'pattern': r'NOK',
        'names': ['norwegian krone', 'krone', 'nok']
    },
    'SEK': {
        'symbols': ['kr', 'SEK'],
        'pattern': r'SEK',
        'names': ['swedish krona', 'krona', 'sek']
    },
    'DKK': {
        'symbols': ['kr', 'DKK'],
        'pattern': r'DKK',
        'names': ['danish krone', 'dkk']
    },
    'PLN': {
        'symbols': ['zł', 'PLN'],
        'pattern': r'zł|PLN',
        'names': ['zloty', 'pln']
    },
    'THB': {
        'symbols': ['฿', 'THB'],
        'pattern': r'฿|THB',
        'names': ['baht', 'thb']
    },
    'MYR': {
        'symbols': ['RM', 'MYR'],
        'pattern': r'RM|MYR',
        'names': ['ringgit', 'myr']
    },
    'IDR': {
        'symbols': ['Rp', 'IDR'],
        'pattern': r'Rp\.?|IDR',
        'names': ['rupiah', 'idr']
    },
    'PHP': {
        'symbols': ['₱', 'PHP'],
        'pattern': r'₱|PHP',
        'names': ['peso', 'php', 'philippine peso']
    },
    'MXN': {
        'symbols': ['$', 'MX$', 'MXN'],
        'pattern': r'MX\$|MXN',
        'names': ['mexican peso', 'mxn']
    },
    'BRL': {
        'symbols': ['R$', 'BRL'],
        'pattern': r'R\$|BRL',
        'names': ['real', 'reais', 'brl', 'brazilian real']
    },
    'ARS': {
        'symbols': ['$', 'ARS'],
        'pattern': r'ARS',
        'names': ['argentine peso', 'ars']
    },
    'CLP': {
        'symbols': ['$', 'CLP'],
        'pattern': r'CLP',
        'names': ['chilean peso', 'clp']
    },
    'COP': {
        'symbols': ['$', 'COL$', 'COP'],
        'pattern': r'COL\$|COP',
        'names': ['colombian peso', 'cop']
    },
    'ZAR': {
        'symbols': ['R', 'ZAR'],
        'pattern': r'ZAR',
        'names': ['rand', 'zar', 'south african rand']
    },
    'RUB': {
        'symbols': ['₽', '₱', 'RUB'],
        'pattern': r'₽|RUB',
        'names': ['ruble', 'rouble', 'rub']
    },
    'TRY': {
        'symbols': ['₺', 'TL', 'TRY'],
        'pattern': r'₺|TRY',
        'names': ['lira', 'try', 'turkish lira']
    },
    'AED': {
        'symbols': ['د.إ', 'DH', 'AED'],
        'pattern': r'AED|DH',
        'names': ['dirham', 'aed', 'emirati dirham']
    },
    'SAR': {
        'symbols': ['﷼', 'SR', 'SAR'],
        'pattern': r'SAR|SR',
        'names': ['riyal', 'sar', 'saudi riyal']
    },
    'QAR': {
        'symbols': ['﷼', 'QR', 'QAR'],
        'pattern': r'QAR|QR',
        'names': ['qatari riyal', 'qar']
    },
    'KWD': {
        'symbols': ['KD', 'KWD'],
        'pattern': r'KWD|KD',
        'names': ['kuwaiti dinar', 'kwd']
    },
    'ILS': {
        'symbols': ['₪', 'ILS'],
        'pattern': r'₪|ILS',
        'names': ['shekel', 'ils', 'israeli shekel']
    },
    'EGP': {
        'symbols': ['E£', 'EGP', 'LE'],
        'pattern': r'E£|EGP|LE',
        'names': ['egyptian pound', 'egp']
    },
    'PKR': {
        'symbols': ['₨', 'Rs', 'PKR'],
        'pattern': r'PKR',
        'names': ['pakistani rupee', 'pkr']
    },
    'BDT': {
        'symbols': ['৳', 'Tk', 'BDT'],
        'pattern': r'৳|BDT',
        'names': ['taka', 'bdt', 'bangladeshi taka']
    },
    'LKR': {
        'symbols': ['Rs', 'LKR'],
        'pattern': r'LKR',
        'names': ['sri lankan rupee', 'lkr']
    },
    'NPR': {
        'symbols': ['Rs', 'NPR'],
        'pattern': r'NPR',
        'names': ['nepalese rupee', 'npr']
    },
    'VND': {
        'symbols': ['₫', 'VND'],
        'pattern': r'₫|VND',
        'names': ['dong', 'vnd', 'vietnamese dong']
    },
    'KZT': {
        'symbols': ['₸', 'KZT'],
        'pattern': r'₸|KZT',
        'names': ['tenge', 'kzt', 'kazakhstani tenge']
    },
    'UAH': {
        'symbols': ['₴', 'UAH'],
        'pattern': r'₴|UAH',
        'names': ['hryvnia', 'uah', 'ukrainian hryvnia']
    },
    'NGN': {
        'symbols': ['₦', 'NGN'],
        'pattern': r'₦|NGN',
        'names': ['naira', 'ngn', 'nigerian naira']
    },
    'KES': {
        'symbols': ['KSh', 'KES'],
        'pattern': r'KSh|KES',
        'names': ['kenyan shilling', 'kes']
    },
    'GHS': {
        'symbols': ['₵', 'GH₵', 'GHS'],
        'pattern': r'₵|GH₵|GHS',
        'names': ['cedi', 'ghs', 'ghanaian cedi']
    },
    'MAD': {
        'symbols': ['DH', 'MAD'],
        'pattern': r'MAD',
        'names': ['moroccan dirham', 'mad']
    },
    'TWD': {
        'symbols': ['NT$', 'TWD'],
        'pattern': r'NT\$|TWD',
        'names': ['new taiwan dollar', 'twd']
    },
    'CZK': {
        'symbols': ['Kč', 'CZK'],
        'pattern': r'Kč|CZK',
        'names': ['koruna', 'czk', 'czech koruna']
    },
    'HUF': {
        'symbols': ['Ft', 'HUF'],
        'pattern': r'Ft|HUF',
        'names': ['forint', 'huf', 'hungarian forint']
    },
    'RON': {
        'symbols': ['lei', 'RON'],
        'pattern': r'RON',
        'names': ['leu', 'ron', 'romanian leu']
    },
    'BGN': {
        'symbols': ['лв', 'BGN'],
        'pattern': r'BGN',
        'names': ['lev', 'bgn', 'bulgarian lev']
    },
    'HRK': {
        'symbols': ['kn', 'HRK'],
        'pattern': r'HRK',
        'names': ['kuna', 'hrk', 'croatian kuna']
    },
}


class CurrencyExtractor:
    
    def __init__(self, default_currency='INR'):
        self.currency_map = CURRENCY_MAP
        self.default_currency = default_currency
        
    def extract_currency(self, text, default=None):
        if not text or str(text).strip() == "" or str(text).strip() == "0":
            return default or self.default_currency
            
        text = str(text)
        
        if re.search(r'G[\s�\ufffd\xa3£]', text):
            return 'GBP'
        
        if '\xa3' in text or '£' in text:
            return 'GBP'
            
        for currency_code, currency_info in self.currency_map.items():
            pattern = currency_info['pattern']
            if re.search(pattern, text, re.IGNORECASE):
                return currency_code
        
        text_lower = text.lower()
        for currency_code, currency_info in self.currency_map.items():
            for name in currency_info['names']:
                if name in text_lower:
                    return currency_code
        
        if re.search(r'\d', text):
            return 'UNKNOWN'
            
        return default or self.default_currency
    
    def extract_currency_symbol(self, text):
        if not text:
            return ''
            
        text = str(text)
        
        if re.search(r'G[\s�\ufffd\xa3£]', text):
            return '£'
        
        symbols = {
            '$': '$',
            '€': '€',
            '£': '£',
            '\xa3': '£',
            'G£': '£',
            '¥': '¥',
            '₹': '₹',
            '₨': '₨',
            '₩': '₩',
            '₱': '₱',
            '₽': '₽',
            '₺': '₺',
            '₪': '₪',
            '₦': '₦',
            '₵': '₵',
            '₴': '₴',
            '₸': '₸',
            '₫': '₫',
            '৳': '৳',
        }
        
        for symbol_code, symbol_display in symbols.items():
            if symbol_code in text:
                return symbol_display
                
        return ''
    
    def get_currency_info(self, currency_code):
        return self.currency_map.get(currency_code.upper())
    
    def is_valid_currency(self, currency_code):
        return currency_code.upper() in self.currency_map
    
    def extract_all_currencies(self, text):
        if not text:
            return []
            
        text = str(text)
        found_currencies = []
        
        if re.search(r'G[\s�\ufffd\xa3£]', text):
            found_currencies.append('GBP')
        
        for currency_code, currency_info in self.currency_map.items():
            if currency_code in found_currencies:
                continue
            pattern = currency_info['pattern']
            if re.search(pattern, text, re.IGNORECASE):
                found_currencies.append(currency_code)
                
        return found_currencies


def extract_currency(text, default='INR'):
    extractor = CurrencyExtractor(default_currency=default)
    return extractor.extract_currency(text)


def extract_currency_symbol(text):
    return extract_currency(text, default='INR')

