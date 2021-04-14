import pandas as pd

#Считываем данные
def open_files(store_nm):
    inventory_path = './input/MS-' + store_nm + '-inventory.csv'
    sell_path = './input/MS-' + store_nm + '-sell.csv'
    supply_path = './input/MS-' + store_nm + '-supply.csv'
    inventory_file = pd.read_csv(inventory_path, header = 0, sep = ',', index_col=False, parse_dates = False)
    sell_file = pd.read_csv(sell_path, header = 0, sep = ',', index_col=False, parse_dates = False)
    supply_file = pd.read_csv(supply_path, header = 0, sep = ',', index_col=False, parse_dates = False)

    return inventory_file, sell_file, supply_file

def sales_count(sell_file):
#Делаем группировку по дате и типу транзакции
    sales_cnt_df = sell_file
    sales_cnt_df['sku_num'] = sales_cnt_df['sku_num'].str.slice(start = 5, stop = 9)
    sales_cnt_df = sales_cnt_df.groupby(sales_cnt_df.columns.tolist(),as_index=False).size()
#Создаем датафрейм с количеством продаж по каждому пункту и обнуляем переменные    
    sales_per_day_df = pd.DataFrame(columns = ['date', 'apple', 'pen'])
    apples_cnt, pens_cnt, i = 0, 0, 0
    curr_date = sales_cnt_df['date'][0]

    for index, line in sales_cnt_df.iterrows():
#Переход на следующую дату, записываем данные за предыдущую дату в датафрейм и обнуляем переменные
        if curr_date != line['date']:
            sales_per_day_df.loc[i] = [curr_date, apples_cnt, pens_cnt]
            i += 1
            curr_date = line['date']
            apples_cnt, pens_cnt = 0, 0
#Считаем количество яблок/ручек, проданных за конкретный день
        if line['sku_num'] == '-ap-':
            apples_cnt = line['size']
        elif line['sku_num'] == '-pe-':
            pens_cnt = line['size']
#Последняя итерация цикла (для последней даты в файле транзакций)
    else:
        i += 1
        sales_per_day_df.loc[i] = [curr_date, apples_cnt, pens_cnt]

    return sales_per_day_df


def inventory_status(sales_df, supply_file, inventory_file):
    inventory_df = pd.DataFrame(columns = ['date', 'apple', 'pen'])
    steals_df = pd.DataFrame(columns = ['date', 'apple', 'pen'])
    apples_cnt, pens_cnt, i = 0, 0, 0
#Пробегаем по файлу продаж и вычитаем количество товара со склада
    for index, line in sales_df.iterrows():
        curr_date = line['date']
        apples_cnt -= int(line['apple'])
        pens_cnt -= int(line['pen'])
#Если в эту дату было поступление, прибавляем его на склад
        if not(supply_file.loc[supply_file['date'] == line['date']].empty):
            apples_cnt += int(supply_file['apple'].loc[supply_file['date'] == line['date']])
            pens_cnt += int(supply_file['pen'].loc[supply_file['date'] == line['date']])
#Конец месяца - заполняем по данным со склада
        if not(inventory_file.loc[inventory_file['date'] == line['date']].empty):
            apples_stolen = apples_cnt - int(inventory_file['apple'].loc[inventory_file['date'] == line['date']])
            pens_stolen = pens_cnt - int(inventory_file['pen'].loc[inventory_file['date'] == line['date']])
            apples_cnt = int(inventory_file['apple'].loc[inventory_file['date'] == line['date']])
            pens_cnt = int(inventory_file['pen'].loc[inventory_file['date'] == line['date']])
            steals_df.loc[i] = [line['date'], apples_stolen, pens_stolen]
            i += 1
        inventory_df.loc[index] = [line['date'], apples_cnt, pens_cnt]
    return inventory_df, steals_df


def best_sales(sales_df):
    sales_grouped = sales_df
    sales_grouped['date'] = pd.to_datetime(sales_grouped['date'])
    sales_grouped= sales_grouped.set_index('date')
#Группируем данные под месяц
    sales_grouped = sales_grouped.groupby([pd.Grouper(freq='M')]).sum()
#Выводим лучшие 3 месяца по продажам яблок и ручек
    sales_grouped.sort_values(by=['apple'], inplace=True, ascending=False)
    print('Лучшие месяцы по продаже яблок')
    print(sales_grouped.iloc[:3])
    print()
    sales_grouped.sort_values(by=['pen'], inplace=True, ascending=False)
    print('Лучшие месяцы по продаже ручек')
    print(sales_grouped.iloc[:3])


def worst_steals(steals_df):
    steals_grouped = steals_df
    steals_grouped['date'] = pd.to_datetime(steals_grouped['date'])
    steals_grouped= steals_grouped.set_index('date')
#Группируем данные под год
    steals_grouped = steals_grouped.groupby([pd.Grouper(freq='Y')]).sum()
    
#Выводим худшие 3 года по кражам яблок и ручек
    steals_grouped.sort_values(by=['apple'], inplace=True, ascending=False)
    print('Самые худшие годы по кражам яблок')
    print(steals_grouped.iloc[:3])
    print()
    steals_grouped.sort_values(by=['pen'], inplace=True, ascending=False)
    print('Самые худшие годы по кражам ручек')
    print(steals_grouped.iloc[:3])


def main(store_nm):
    print('##############')
    print(store_nm)
    print('##############')
#Открываем файлы
    inventory_file, sell_file, supply_file = open_files(store_nm)
#Формируем список продаж
    sales_df = sales_count(sell_file)
#Формируем состояние склада и считаем кражи
    inventory_df, steals_df = inventory_status(sales_df, supply_file, inventory_file)
    inventory_df.to_csv(r'./output/result_MS-' + store_nm + '-inventory.csv', index = False)
    steals_df.to_csv(r'./output/result_MS-' + store_nm + '-steals.csv', index = False)
#Выводим лучшие месяцы по продажам
    best_sales(sales_df)
#Выводим самые худшие года по кражам
    worst_steals(steals_df)
    print('##############')
    print('##############')


store_names = ['b1', 'b2', 'm1', 'm2', 's1', 's2', 's3', 's4', 's5']
for nm in store_names:
    main(nm)




