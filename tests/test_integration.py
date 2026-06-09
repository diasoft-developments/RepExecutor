"""Интеграционный тест: format_number_with_words + parse_twr_digit_fields"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from parsers.wrd_field_formatter import format_number_with_words
from parsers.wrd_parser import parse_twr_digit_fields


def test_basic():
    print("=== Базовые тесты ===")
    
    # Тест 1: простое число с IntParts (DFM формат)
    dfm_text = '''
        object SumWr: TWRDigitField
            FieldName = 'SumWr'
            DisplayLabel = 'SumWr'
            IntPart.Strings = (
              '#1088'#1077#1083#1100
              '#1088'#1077#1083#1072
              '#1088'#1077#1083#1077#1081)
            FracPart.Strings = (
              ''
              ''
              '')
            DataField = 'Sum'
        end
    '''
    fields = parse_twr_digit_fields(dfm_text)
    print(f"  Найдено полей: {len(fields)}")
    for fn, f in fields.items():
        print(f"  Поле '{fn}': IntParts={f.IntParts}, DataField='{f.DataField}'")
    result = format_number_with_words(fields, 'SumWr', 150)
    print(f"  SumWr = 150: {result}")
    
    # Тест 2: дробное число 0.1 (десятые)
    dfm_text2 = '''
        object AmountWr: TWRDigitField
            FieldName = 'AmountWr'
            DisplayLabel = 'AmountWr'
            IntPart.Strings = (
              '#1088'#1077#1083#1100
              '#1088'#1077#1083#1072
              '#1088'#1077#1083#1077#1081)
            FracPart.Strings = (
              ''
              ''
              '')
            DataField = 'Amount'
        end
    '''
    fields = parse_twr_digit_fields(dfm_text2)
    result = format_number_with_words(fields, 'AmountWr', 0.1)
    print(f"  AmountWr = 0.1: {result}")
    
    # Тест 3: дробное число 1.5 (десятые)
    result = format_number_with_words(fields, 'AmountWr', 1.5)
    print(f"  AmountWr = 1.5: {result}")
    
    # Тест 4: дробное число 1.23 (сотые)
    result = format_number_with_words(fields, 'AmountWr', 1.23)
    print(f"  AmountWr = 1.23: {result}")
    
    # Тест 5: с FracPartDefinition=False
    dfm_text3 = '''
        object PriceWr: TWRDigitField
            FieldName = 'PriceWr'
            DisplayLabel = 'PriceWr'
            IntPart.Strings = (
              '#1088'#1077#1083#1100
              '#1088'#1077#1083#1072
              '#1088'#1077#1083#1077#1081)
            FracPart.Strings = (
              ''
              ''
              '')
            FracPartDefinition = False
            DataField = 'Price'
        end
    '''
    fields = parse_twr_digit_fields(dfm_text3)
    result = format_number_with_words(fields, 'PriceWr', 50.7)
    print(f"  PriceWr = 50.7 (FracPartDefinition=False): {result}")
    
    # Тест 6: с FracParts
    dfm_text4 = '''
        object RubleWr: TWRDigitField
            FieldName = 'RubleWr'
            DisplayLabel = 'RubleWr'
            IntPart.Strings = (
              '#1088'#1091#1073#1083#1100
              '#1088'#1091#1073#1083#1100
              '#1088'#1091#1073#1083#1077#1081)
            FracPart.Strings = (
              '#1082'#1086#1087#1077#1081#1082#1072
              '#1082#1086#1087#1077#1081#1082#1081
              '#1082#1086#1087#1077#1081#1082)
            DataField = 'Ruble'
        end
    '''
    fields = parse_twr_digit_fields(dfm_text4)
    for fn, f in fields.items():
        print(f"  Поле '{fn}': FracParts={f.FracParts}")
    result = format_number_with_words(fields, 'RubleWr', 100.50)
    print(f"  RubleWr = 100.50 с FracParts: {result}")
    
    # Тест 7: без параметров (дефолт)
    dfm_text5 = '''
        object DefaultWr: TWRDigitField
            FieldName = 'DefaultWr'
            DisplayLabel = 'DefaultWr'
            IntPart.Strings = (
              ''
              ''
              '')
            FracPart.Strings = (
              ''
              ''
              '')
            DataField = 'DefVal'
        end
    '''
    fields = parse_twr_digit_fields(dfm_text5)
    result = format_number_with_words(fields, 'DefaultWr', 999)
    print(f"  DefaultWr = 999 без параметров: {result}")


if __name__ == '__main__':
    test_basic()