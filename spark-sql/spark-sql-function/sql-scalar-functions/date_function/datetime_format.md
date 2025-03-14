# Format

Symbol |Meaning |Presentation |Examples
--|---|---|--
G |era |text| AD; Anno Domini
y |year| year| 2020; 20
D |day-of-year| number(3)| 189
M/L |month-of-year |month| 7; 07; Jul; July
d |day-of-month| number(2)| 28
Q/q |quarter-of-year| number/text| 3; 03; Q3; 3rd quarter
E |day-of-week| text| Tue; Tuesday
F |aligned day of week in month| number(1)| 3
a |am-pm-of-day| am-pm| PM
h |clock-hour-of-am-pm (1-12)| number(2)| 12
K |hour-of-am-pm (0-11)| number(2)| 0
k |clock-hour-of-day (1-24)| number(2)| 1
H |hour-of-day (0-23)| number(2)| 0
m |minute-of-hour| number(2)| 30
s |second-of-minute| number(2) |55
S |fraction-of-second| fraction| 978
V |time-zone ID |zone-id |America/Los_Angeles; Z; -08:30
z |time-zone name| zone-name| Pacific Standard Time; PST
O |localized zone-offset |offset-O| GMT+8; GMT+08:00 ;UTC-08:00;
X |zone-offset 'Z' for zero |offset-X| Z; -08; -0830; -08:30; -083015; -08:30:15;
x |zone-offset |offset-x| +0000; -08; -0830; -08:30; -083015; -08:30:15;
Z |zone-offset |offset-Z |+0000; -0800; -08:00;
' |escape for text |delimiter|  
''| single quote |literal |'
[ |optional section start||
] |optional section end ||

## Text

The text style is determined based on the number of pattern letters used.

Less than 4 pattern letters will use the short text form, typically an abbreviation, e.g. day-of-week Monday might output `Mon`.

Exactly 4 pattern letters will use the full text form, typically the full description, e.g, day-of-week Monday might output `Monday`.

5 or more letters will fail.

## Number(n)

The n here represents the maximum count of letters this type of datetime pattern can be used

In formatting, if the count of letters is one, then the value is output using the minimum number of digits and without padding otherwise, the count of digits is used as the width of the output field, with the value zero-padded as necessary.

In parsing, the exact count of digits is expected in the input field.

## Number/Text

If the count of pattern letters is 3 or greater, use the Text rules above. Otherwise use the Number rules above

## Fraction

Use one or more (up to 9) contiguous 'S' characters, e,g SSSSSS, to parse and format fraction of second.

For parsing, the acceptable fraction length can be [1, the number of contiguous ‘S’].

For formatting, the fraction length would be padded to the number of contiguous ‘S’ with zeros.

Spark supports datetime of micro-of-second precision, which has up to 6 significant digits, but can parse nano-of-second with exceeded part truncated.

## Year

The count of letters determines the minimum field width below which padding is used. If the count of letters is two, then a reduced two digit form is used. For printing, this outputs the rightmost two digits. For parsing, this will parse using the base value of 2000, resulting in a year within the range 2000 to 2099 inclusive. If the count of letters is less than four (but not two), then the sign is only output for negative years. Otherwise, the sign is output if the pad width is exceeded when ‘G’ is not present. 7 or more letters will fail.

## Month

### 'M' or 'L'

Month number in a year starting from 1.

There is no difference between 'M' and 'L'.

Month from 1 to 9 are printed without padding

    select date_format(date '1970-01-01', "M");
    select date_format(date '1970-12-01', "L");

### 'MM' or 'LL'

Month number in a year starting from 1. Zero padding is added for month 1-9.

    select date_format(date '1970-1-01', "LL");
    select date_format(date '1970-09-01', "MM");

### 'MMM'

Short textual representation in the standard form.

    select date_format(date '1970-01-01', "d MMM");
    select to_csv(named_struct('date', date '1970-01-01'), map('dateFormat', 'dd MMM', 'locale', 'RU'));

### 'LLL'

Short textual representation in the stand-alone form. It should be used to format/parse only months without any other date fields.

    select date_format(date '1970-01-01', "LLL");
    select to_csv(named_struct('date', date '1970-01-01'), map('dateFormat', 'LLL', 'locale', 'RU'));

### 'MMMM'

full textual month representation in the standard form. It is used for parsing/formatting months as a part of dates/timestamps.

    select date_format(date '1970-01-01', "d MMMM");
    select to_csv(named_struct('date', date '1970-01-01'), map('dateFormat', 'd MMMM', 'locale', 'RU'));

### 'LLLL'

full textual month representation in the stand-alone form. The pattern can be used to format/parse only months.

    select date_format(date '1970-01-01', "LLLL");
    select to_csv(named_struct('date', date '1970-01-01'), map('dateFormat', 'LLLL', 'locale', 'RU'));
