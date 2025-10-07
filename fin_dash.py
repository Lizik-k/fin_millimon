from sqlalchemy import create_engine, text
import os
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
import calendar
import plotly.express as px

engine = create_engine(
    st.secrets["DB_URL"],
    connect_args={"sslmode": "require"},
    pool_pre_ping=True
)

# with sqlite3.connect('millimon_finsnce.db') as db:
#     cur = db.cursor()
#     cur.execute("PRAGMA foreign_keys = ON")
#
#     # Таблица вида финансов
#     cur.execute('''
#     CREATE TABLE IF NOT EXISTS operation_types (
#         id_operation INTEGER PRIMARY KEY AUTOINCREMENT,
#         name_operation VARCHAR(10) NOT NULL UNIQUE CHECK (name_operation IN ('доход', 'расход')),
#         description TEXT
#     )
#     ''')
#
#     # 2. Таблица категорий (теперь ссылается на тип операции)
#     cur.execute('''
#     CREATE TABLE IF NOT EXISTS categories (
#         id_categories INTEGER PRIMARY KEY AUTOINCREMENT,
#         operation_type_id INTEGER NOT NULL,
#         name VARCHAR(100) NOT NULL UNIQUE,
#         description TEXT,
#         FOREIGN KEY (operation_type_id) REFERENCES operation_types(id_operation) ON DELETE CASCADE
#     )
#     ''')
#
#     # 3. Таблица подкатегорий
#     cur.execute('''
#         CREATE TABLE IF NOT EXISTS subcategories (
#             id_subcategories INTEGER PRIMARY KEY AUTOINCREMENT,
#             category_id INTEGER NOT NULL,
#             name VARCHAR(100) NOT NULL,
#             description TEXT,
#             UNIQUE (category_id, name),
#             FOREIGN KEY (category_id) REFERENCES categories(id_categories) ON DELETE CASCADE
#         )
#         ''')
#
#     # 4. Таблица групп
#     cur.execute('''
#         CREATE TABLE IF NOT EXISTS groups (
#             id_groups INTEGER PRIMARY KEY AUTOINCREMENT,
#             subcategory_id INTEGER NOT NULL,
#             name VARCHAR(100) NOT NULL,
#             description TEXT,
#             UNIQUE (subcategory_id, name),
#             FOREIGN KEY (subcategory_id) REFERENCES subcategories(id_subcategories) ON DELETE CASCADE
#         )
#         ''')
#
#     # 5. Таблица подгрупп
#     cur.execute('''
#         CREATE TABLE IF NOT EXISTS subgroups (
#             id_subgroups INTEGER PRIMARY KEY AUTOINCREMENT,
#             group_id INTEGER NOT NULL,
#             name VARCHAR(100) NOT NULL,
#             description TEXT,
#             UNIQUE (group_id, name),
#             FOREIGN KEY (group_id) REFERENCES groups(id_groups) ON DELETE CASCADE
#         )
#         ''')
#
#     # 6. Таблица финансовых операций
#     cur.execute('''
#        CREATE TABLE IF NOT EXISTS financial_operations (
#            id INTEGER PRIMARY KEY AUTOINCREMENT,
#            operation_date DATE NOT NULL,
#            operation_type_id INTEGER NOT NULL,
#            amount DECIMAL(15, 2) NOT NULL CHECK (amount >= 0),
#            category_id INTEGER NOT NULL,
#            subcategory_id INTEGER,
#            group_id INTEGER,
#            subgroup_id INTEGER,
#            comment TEXT,
#            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#            FOREIGN KEY (operation_type_id) REFERENCES operation_types(id_operation),
#            FOREIGN KEY (category_id) REFERENCES categories(id_categories),
#            FOREIGN KEY (subcategory_id) REFERENCES subcategories(id_subcategories),
#            FOREIGN KEY (group_id) REFERENCES groups(id_groups),
#            FOREIGN KEY (subgroup_id) REFERENCES subgroups(id_subgroups)
#        )
#        ''')
#
#
#
#
#     # Триггер для обновления времени
#     cur.execute('''
#         CREATE TRIGGER IF NOT EXISTS update_operations_timestamp
#         AFTER UPDATE ON financial_operations
#         FOR EACH ROW
#         BEGIN
#             UPDATE financial_operations SET updated_at = CURRENT_TIMESTAMP WHERE id = OLD.id;
#         END
#         ''')

class FinanceApp:

    def __init__(self, db_url=None):
        if db_url is None:
            # Streamlit Cloud: st.secrets
            try:
                db_url = st.secrets["DB_URL"]
            except Exception:
                db_url = os.getenv("DB_URL")
        if not db_url:
            raise RuntimeError("DB_URL не задан")
            # Подключаемся с sslmode (Supabase)
        self.engine = create_engine(db_url, connect_args={"sslmode": "require"})


    def get_connection(self):
        # """Устанавливает соединение с базой данных"""
        # conn = sqlite3.connect(self.db_name)
        # conn.execute("PRAGMA foreign_keys = ON")
        # return conn
        """Возвращает SQLAlchemy connection (использовать с pandas.read_sql и .execute)"""
        return self.engine.connect()


    def get_operation_types(self):
        """Получает список всех типов операций"""
        conn = self.get_connection()
        df = pd.read_sql("SELECT id_operation, name_operation, description FROM operation_types", conn)
        conn.close()
        return df


    def get_categories(self, operation_type_id=None):
        """Получает список категорий"""
        conn = self.get_connection()
        if operation_type_id:
            query = "SELECT id_categories, name FROM categories WHERE operation_type_id = ?"
            df = pd.read_sql(query, conn, params=(int(operation_type_id),))
        else:
            df = pd.read_sql("SELECT id_categories, name, operation_type_id FROM categories", conn)
        conn.close()
        return df


    def get_subcategories(self, category_id):
        """Получает список подкатегорий для категории"""
        conn = self.get_connection()
        query = "SELECT id_subcategories, name FROM subcategories WHERE category_id = ?"
        df = pd.read_sql(query, conn, params=(category_id,))
        conn.close()
        return df


    def add_operation(self, operation_data):
        """Добавляет новую финансовую операцию (operation_data — tuple)"""
        # operation_data: (operation_date, type_id, amount, category_id, subcat_id, group_id, subgroup_id, comment, lesson_type_id?)
        insert_sql = text('''
            INSERT INTO financial_operations
            (operation_date, operation_type_id, amount, category_id, subcategory_id, group_id, subgroup_id, comment, lesson_type_id)
            VALUES (:operation_date, :operation_type_id, :amount, :category_id, :subcategory_id, :group_id, :subgroup_id, :comment, :lesson_type_id)
        ''')
        params = {
            "operation_date": operation_data[0],
            "operation_type_id": operation_data[1],
            "amount": operation_data[2],
            "category_id": operation_data[3],
            "subcategory_id": operation_data[4],
            "group_id": operation_data[5],
            "subgroup_id": operation_data[6],
            "comment": operation_data[7],
            "lesson_type_id": operation_data[8] if len(operation_data) > 8 else None
        }
        try:
            with self.engine.begin() as conn:
                conn.execute(insert_sql, params)
            return True
        except Exception as e:
            st.error(f"Ошибка при добавлении операции: {e}")
            return False

    def get_operations(self, start_date=None, end_date=None):
        """Получает операции за период с правильными JOIN по вашей схеме"""
        conn = self.get_connection()

        query = '''
        SELECT 
            f.id,
            f.operation_date,
            ot.name_operation AS operation_type,
            c.name AS category,
            s.name AS subcategory,
            g.name AS group_name,
            sg.name AS subgroup,
            lt.name AS lesson_type,
            f.amount,
            f.comment,
            f.created_at,
            f.updated_at
        FROM financial_operations f
        JOIN operation_types ot ON f.operation_type_id = ot.id_operation
        JOIN categories c ON f.category_id = c.id_categories
        LEFT JOIN subcategories s ON f.subcategory_id = s.id_subcategories
        LEFT JOIN groups g ON f.group_id = g.id_groups
        LEFT JOIN subgroups sg ON f.subgroup_id = sg.id_subgroups
        LEFT JOIN lesson_types lt ON f.lesson_type_id = lt.id_lesson_type
    '''

        params = []
        if start_date and end_date:
            query += " WHERE f.operation_date BETWEEN ? AND ?"
            params.extend([start_date, end_date])

        query += " ORDER BY f.operation_date DESC, f.id DESC"

        df = pd.read_sql(query, conn, params=params)
        conn.close()
        return df


    def get_financial_summary(self, start_date=None, end_date=None):
        """Получает финансовую сводку"""
        conn = self.get_connection()

        query = '''
            SELECT 
                ot.name as operation_type,
                c.name as category,
                SUM(f.amount) as total
            FROM financial_operations f
            JOIN operation_types ot ON f.operation_type_id = ot.id
            JOIN categories c ON f.category_id = c.id
            '''

        params = []
        if start_date and end_date:
            query += " WHERE f.operation_date BETWEEN ? AND ?"
            params.extend([start_date, end_date])

        query += " GROUP BY ot.name, c.name ORDER BY total DESC"

        df = pd.read_sql(query, conn, params=params)
        conn.close()
        return df


    def get_monthly_summary(self):
        """Получает помесячную статистику"""
        conn = self.get_connection()

        query = '''
            SELECT 
                strftime('%Y-%m', operation_date) as month,
                ot.name as operation_type,
                SUM(amount) as total
            FROM financial_operations f
            JOIN operation_types ot ON f.operation_type_id = ot.id
            GROUP BY month, operation_type
            ORDER BY month
            '''

        df = pd.read_sql(query, conn)
        conn.close()
        return df

def import_excel_to_db(app, df):
    """
    Импортирует данные из Excel в БД.
    Добавляет новые категории / подкатегории / группы / подгруппы / типы занятий, если их нет.
    """

    conn = app.get_connection()
    cur = conn.cursor()

    inserted_count = 0

    for _, row in df.iterrows():
        try:
            # === 1️⃣ Определяем тип операции ===
            cur.execute("SELECT id_operation FROM operation_types WHERE name_operation = ?", (row['Тип операции'],))
            type_row = cur.fetchone()
            if not type_row:
                cur.execute("INSERT INTO operation_types (name_operation) VALUES (?)", (row['Тип операции'],))
                conn.commit()
                type_id = cur.lastrowid
            else:
                type_id = type_row[0]

            # === 2️⃣ Категория ===
            cur.execute("SELECT id_categories FROM categories WHERE name = ?", (row['Категория'],))
            cat_row = cur.fetchone()
            if not cat_row:
                cur.execute("INSERT INTO categories (operation_type_id, name) VALUES (?, ?)", (type_id, row['Категория']))
                conn.commit()
                category_id = cur.lastrowid
            else:
                category_id = cat_row[0]

            # === 3️⃣ Подкатегория ===
            subcat_id = None
            if pd.notna(row['Подкатегория']):
                cur.execute(
                    "SELECT id_subcategories FROM subcategories WHERE category_id = ? AND name = ?",
                    (category_id, row['Подкатегория'])
                )
                s_row = cur.fetchone()
                if not s_row:
                    cur.execute("INSERT INTO subcategories (category_id, name) VALUES (?, ?)",
                                (category_id, row['Подкатегория']))
                    conn.commit()
                    subcat_id = cur.lastrowid
                else:
                    subcat_id = s_row[0]

            # === 4️⃣ Группа ===
            group_id = None
            if pd.notna(row['Группа']):
                cur.execute(
                    "SELECT id_groups FROM groups WHERE subcategory_id = ? AND name = ?",
                    (subcat_id, row['Группа'])
                )
                g_row = cur.fetchone()
                if not g_row:
                    cur.execute("INSERT INTO groups (subcategory_id, name) VALUES (?, ?)", (subcat_id, row['Группа']))
                    conn.commit()
                    group_id = cur.lastrowid
                else:
                    group_id = g_row[0]

            # === 5️⃣ Подгруппа ===
            subgroup_id = None
            if pd.notna(row['Подгруппа']):
                cur.execute(
                    "SELECT id_subgroups FROM subgroups WHERE group_id = ? AND name = ?",
                    (group_id, row['Подгруппа'])
                )
                sg_row = cur.fetchone()
                if not sg_row:
                    cur.execute("INSERT INTO subgroups (group_id, name) VALUES (?, ?)", (group_id, row['Подгруппа']))
                    conn.commit()
                    subgroup_id = cur.lastrowid
                else:
                    subgroup_id = sg_row[0]

            # === 6️⃣ Тип занятия ===
            lesson_type_id = None
            if pd.notna(row['Тип занятия']):
                cur.execute("SELECT id_lesson_type FROM lesson_types WHERE name = ?", (row['Тип занятия'],))
                lt_row = cur.fetchone()
                if not lt_row:
                    cur.execute("INSERT INTO lesson_types (name) VALUES (?)", (row['Тип занятия'],))
                    conn.commit()
                    lesson_type_id = cur.lastrowid
                else:
                    lesson_type_id = lt_row[0]

            # === 7️⃣ Добавляем саму операцию ===
            cur.execute('''
                INSERT INTO financial_operations 
                (operation_date, operation_type_id, amount, category_id, subcategory_id, group_id, subgroup_id, lesson_type_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                pd.to_datetime(row['Дата']).strftime('%Y-%m-%d'),
                type_id,
                float(row['Сумма']),
                category_id,
                subcat_id,
                group_id,
                subgroup_id,
                lesson_type_id
            ))

            inserted_count += 1

        except Exception as e:
            st.error(f"Ошибка при добавлении строки: {e}")

    conn.commit()
    conn.close()
    return inserted_count

def main():
    st.set_page_config(
        page_title="Финансы онлайн-школы",
        page_icon="💰",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    app = FinanceApp()

    # Сайдбар с навигацией

    page = st.sidebar.radio("Навигация", ["Дашборд", "Журнал операций"])

    # --- Загрузка Excel-файла ---
    st.sidebar.header("Импорт данных")
    uploaded_file = st.sidebar.file_uploader("Загрузите Excel-файл (.xlsx)", type=["xlsx"])

    if uploaded_file:
        st.sidebar.info("Файл загружен. Нажмите кнопку для импорта данных в базу.")
        if st.sidebar.button("📤 Импортировать данные"):
            try:
                df_new = pd.read_excel(uploaded_file)

                required_columns = [
                    'Дата', 'Тип операции', 'Сумма', 'Категория',
                    'Подкатегория', 'Группа', 'Подгруппа', 'Тип занятия'
                ]
                if not all(col in df_new.columns for col in required_columns):
                    st.error("Ошибка: в файле отсутствуют обязательные столбцы.")
                else:
                    imported = import_excel_to_db(app, df_new)
                    st.success(f"Импорт завершён! Добавлено {imported} операций.")
            except Exception as e:
                st.error(f"Ошибка при обработке файла: {e}")

    # Период для анализа
    st.sidebar.header("Период анализа")
    today = datetime.now()
    first_day_of_month = today.replace(day=1)
    last_day_of_month = today.replace(day=calendar.monthrange(today.year, today.month)[1])

    date_range = st.sidebar.selectbox(
        "Выберите период",
        ["Текущий месяц", "Текущая неделя", "Сегодня", "Произвольный период"]
    )

    start_date, end_date = first_day_of_month, last_day_of_month

    if date_range == "Сегодня":
        start_date = end_date = today.strftime('%Y-%m-%d')
    elif date_range == "Текущая неделя":
        start_date = (today - timedelta(days=today.weekday())).strftime('%Y-%m-%d')
        end_date = today.strftime('%Y-%m-%d')
    elif date_range == "Произвольный период":
        col1, col2 = st.sidebar.columns(2)
        with col1:
            start_date = st.date_input("Начальная дата", first_day_of_month)
        with col2:
            end_date = st.date_input("Конечная дата", last_day_of_month)
        start_date = start_date.strftime('%Y-%m-%d')
        end_date = end_date.strftime('%Y-%m-%d')
    else:  # Текущий месяц
        start_date = first_day_of_month.strftime('%Y-%m-%d')
        end_date = last_day_of_month.strftime('%Y-%m-%d')

    if page == "Дашборд":
        st.title("Дашборд финансов")
        # Загружаем операции
        df = app.get_operations(start_date, end_date)

        if df.empty:
            st.warning("Нет данных за выбранный период.")
        else:
            # Преобразуем типы
            df['operation_date'] = pd.to_datetime(df['operation_date'])
            df['amount'] = df['amount'].astype(float)

            # === 1️⃣ Общие показатели ===
            total_income = df.loc[df['operation_type'] == 'доход', 'amount'].sum()
            total_expense = df.loc[df['operation_type'] == 'расход', 'amount'].sum()
            profit = total_income - total_expense
            operations_count = len(df)

            st.subheader("Ключевые метрики")
            c1, c2, c3 = st.columns(3)

            # c1.metric("Доходы", f"{total_income:,.2f} ₽")
            # c2.metric("Расходы", f"{total_expense:,.2f} ₽")
            # c3.metric("Прибыль", f"{profit:,.2f} ₽")
            c1.metric("Доходы", f"{total_income:,.2f}".replace(",", " ").replace(".", ",") + " ₽")
            c2.metric("Расходы", f"{total_expense:,.2f}".replace(",", " ").replace(".", ",") + " ₽")
            c3.metric("Прибыль", f"{profit:,.2f}".replace(",", " ").replace(".", ",") + " ₽")


            # === 2️⃣ Динамика доходов и расходов по времени ===
            st.subheader("Динамика доходов и расходов")
            df_month = df.copy()
            df_month['month'] = df_month['operation_date'].dt.to_period('M').astype(str)
            monthly = df_month.groupby(['month', 'operation_type'])['amount'].sum().reset_index()
            pivot = monthly.pivot(index='month', columns='operation_type', values='amount').fillna(0)

            import plotly.graph_objects as go

            line_fig = go.Figure()

            # Линия расходов (красная)
            if 'расход' in pivot.columns:
                line_fig.add_trace(go.Scatter(
                    x=pivot.index,
                    y=pivot['расход'],
                    mode='lines+markers',
                    name='Расход',
                    line=dict(color='red', width=2),
                    fill='tozeroy',
                    fillcolor='rgba(255, 0, 0, 0.2)'
                ))

            # Линия доходов (зелёная)
            if 'доход' in pivot.columns:
                line_fig.add_trace(go.Scatter(
                    x=pivot.index,
                    y=pivot['доход'],
                    mode='lines+markers',
                    name='Доход',
                    line=dict(color='green', width=2),
                    fill='tozeroy',
                    fillcolor='rgba(0, 200, 0, 0.2)'
                ))


            line_fig.update_layout(
                title="Динамика по месяцам",
                xaxis_title="Месяц",
                yaxis_title="Сумма (₽)",
                template="plotly_white",
                hovermode="x unified"
            )

            st.plotly_chart(line_fig, use_container_width=True)

            st.subheader("Структура расходов по категориям")
            expense_df = df[df['operation_type'] == 'расход']

            if not expense_df.empty:
                #import plotly.express as px
                # Создаём два столбца
                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("**Общая структура расходов по категориям**")
                    cat_expense = (
                        expense_df.groupby('category')['amount']
                        .sum()
                        .reset_index()
                        .sort_values('amount', ascending=False)
                    )

                    pie_chart_total = px.pie(
                        cat_expense,
                        names='category',
                        values='amount',
                        title="По категориям",
                        hole=0.4
                    )
                    pie_chart_total.update_traces(textinfo='percent+label')
                    st.plotly_chart(pie_chart_total, use_container_width=True)

                with col2:


                    # Выпадающий список категорий
                    categories = sorted(expense_df['category'].dropna().unique())
                    selected_category = st.selectbox(
                        "Выберите категорию расходов:",
                        options=categories,
                        index=0 if categories else None
                    )

                    filtered = expense_df[expense_df['category'] == selected_category]

                    # Группируем подкатегории
                    sub_expense = (
                        filtered.groupby('subcategory')['amount']
                        .sum()
                        .reset_index()
                        .sort_values('amount', ascending=False)
                    )

                    pie_chart_sub = px.pie(
                        sub_expense,
                        names='subcategory',
                        values='amount',
                        title=f"Подкатегории — {selected_category}",
                        hole=0.4
                    )
                    pie_chart_sub.update_traces(textinfo='percent')
                    st.plotly_chart(pie_chart_sub, use_container_width=True)
            else:
                st.info("Нет данных по расходам для выбранного периода.")

            # === 5️⃣ Доход по фильтрам ===
            st.subheader("Анализ доходов по фильтрам")

            income_df = df[df['operation_type'] == 'доход']
            # st.write(income_df)
            if not income_df.empty:
                col1, col2, col3, cols4 = st.columns(4)

                # === Фильтр по категории ===
                with col1:
                    categories = sorted(income_df['category'].dropna().unique())
                    selected_category = st.selectbox(
                        "Выберите категорию доходов:",
                        options=["Все"] + categories,
                        index=0
                    )
                    if selected_category != "Все":
                        income_df = income_df[income_df['category'] == selected_category]

                # === Фильтр по подкатегории ===
                with col2:
                    subcategories = sorted(income_df['subcategory'].dropna().unique())
                    selected_subcategory = st.selectbox(
                        "Выберите подкатегорию доходов:",
                        options=["Все"] + subcategories,
                        index=0
                    )
                    if selected_subcategory != "Все":
                        income_df = income_df[income_df['subcategory'] == selected_subcategory]

                # === Фильтр по группе ===
                with col3:
                    groups = sorted(income_df['group_name'].dropna().unique())
                    selected_group = st.selectbox(
                        "Выберите подкатегорию доходов:",
                        options=["Все"] + groups,
                        index=0
                    )
                    if selected_group != "Все":
                        income_df = income_df[income_df['group_name'] == selected_group]

                # === Фильтр по типу занятия ===
                with cols4:
                    lesson_types = sorted(income_df['lesson_type'].dropna().unique())
                    selected_lesson_type = st.selectbox(
                        "Тип занятия:",
                        options=["Все"] + lesson_types,
                        index=0
                    )
                    if selected_lesson_type != "Все":
                        income_df = income_df[income_df['lesson_type'] == selected_lesson_type]

                # === Итоговая группировка ===
                grouped_income = (
                    income_df.groupby('subcategory')['amount']
                    .sum()
                    .reset_index()
                    .sort_values('amount', ascending=False)
                )

                if not grouped_income.empty:
                    pie_chart = px.pie(
                        grouped_income,
                        names='subcategory',
                        values='amount',
                        title="Структура доходов по подкатегориям",
                        hole=0.4
                    )
                    pie_chart.update_traces(textinfo='percent+label')
                    pie_chart.update_layout(template="plotly_white")

                    st.plotly_chart(pie_chart, use_container_width=True)
                else:
                    st.info("Нет данных для выбранных фильтров.")
            else:
                st.info("Нет данных по доходам для выбранного периода.")

            # === 6️⃣ Кумулятивная прибыль ===
            st.subheader("Кумулятивная прибыль")
            df_sorted = df.sort_values('operation_date')
            df_sorted['profit'] = df_sorted.apply(
                lambda x: x['amount'] if x['operation_type'] == 'доход' else -x['amount'], axis=1)
            df_sorted['cum_profit'] = df_sorted['profit'].cumsum()

            profit_fig = px.area(
                df_sorted,
                x='operation_date',
                y='cum_profit',
                title="Накопительная прибыль",
                labels={'cum_profit': 'Накопленная прибыль (₽)', 'operation_date': 'Дата'}
            )
            st.plotly_chart(profit_fig, use_container_width=True)
    # Журнал операций
    elif page == "Журнал операций":
        st.title("📋 Журнал операций")
        # --- ФИЛЬТРЫ ---
        cols1, cols2, cols3, cols4 = st.columns(4)
        with cols1:
            # Тип операции
            op_types_df = app.get_operation_types()
            op_type_map = dict(zip(op_types_df['name_operation'], op_types_df['id_operation']))
            selected_type = st.multiselect(
                "Тип операции",
                options=list(op_type_map.keys())
            )
        with cols2:
            # Категория
            all_cats_df = app.get_categories()
            cat_map = dict(zip(all_cats_df['name'], all_cats_df['id_categories']))
            selected_cat = st.multiselect(
                "Категория",
                options=list(cat_map.keys())
            )
        with cols3:
            cols3_1, cols3_2 = st.columns(2)
            with cols3_1:
                # Диапазон сумм
                min_amount = st.number_input("Сумма от", min_value=0.0, value=0.0, step=100.0)
            with cols3_2:
                max_amount = st.number_input("Сумма до", min_value=0.0, value=1000000.0, step=100.0)

        with cols4:
            # Сколько строк показывать
            limit = st.selectbox("Показывать записей", [10, 50, 100, 500], index=2)



        # --- ЗАГРУЗКА ДАННЫХ ---
        df = app.get_operations(start_date, end_date)

        # Приведение типов
        if not df.empty:
            df['amount'] = df['amount'].astype(float)
            df['operation_date'] = pd.to_datetime(df['operation_date']).dt.date

            # Фильтрация
            if selected_type:
                df = df[df['operation_type'].isin(selected_type)]
            if selected_cat:
                df = df[df['category'].isin(selected_cat)]
            if min_amount or max_amount:
                df = df[(df['amount'] >= min_amount) & (df['amount'] <= max_amount)]

            # Сортировка по дате
            df = df.sort_values(by='operation_date', ascending=False)

            # Ограничение кол-ва строк
            df = df.head(limit)

        # --- ВЫВОД ТАБЛИЦЫ ---
        st.subheader("Последние операции")
        if df.empty:
            st.info("Нет операций за выбранный период и условия.")
        else:
            # Немного косметики
            df_view = df[[
                'operation_date', 'operation_type', 'category', 'subcategory',
                'group_name', 'subgroup', 'lesson_type', 'amount', 'comment', 'created_at'
            ]]
            df_view.columns = [
                'Дата', 'Тип', 'Категория', 'Подкатегория',
                'Группа', 'Подгруппа', 'Тип занятия', 'Сумма', 'Комментарий', 'Создано'
            ]
            st.dataframe(df_view, use_container_width=True, hide_index=True)

            # Кнопка выгрузки CSV
            csv = df_view.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Скачать как CSV",
                data=csv,
                file_name="operations_journal.csv",
                mime="text/csv",
            )

    #     # Добавление операции
    # elif page == "Добавить операцию":
    #     st.title("➕ Добавить операцию")
    #     c1, c2, c3 = st.columns(3)
    #     with c1:
    #         # --- Ввод даты и суммы ---
    #         operation_date = st.date_input("Дата операции", datetime.now()).strftime('%Y-%m-%d')
    #     with c2:
    #         # --- Тип операции ---
    #         types_df = app.get_operation_types()
    #         type_names = types_df['name_operation'].tolist()
    #
    #         selected_type = st.selectbox("Выберите тип операции", type_names)
    #         type_id = None
    #         if selected_type in type_names:
    #             type_id = int(types_df.loc[types_df['name_operation'] == selected_type, 'id_operation'].values[0])
    #     with c3:
    #         amount = st.number_input("Сумма", min_value=0.0, step=0.01, format="%.2f")
    #
    #
    #     # --- Категория ---
    #     st.subheader("Категория")
    #     categories_df = app.get_categories(type_id) if type_id else pd.DataFrame(columns=['id_categories', 'name'])
    #     category_names = categories_df['name'].tolist()
    #
    #     selected_category = st.selectbox("Выберите категорию", category_names + ["➕ Добавить новую"])
    #
    #     category_id = None
    #     if selected_category != "➕ Добавить новую" and not categories_df.empty:
    #         category_id = int(categories_df.loc[categories_df['name'] == selected_category, 'id_categories'].values[0])
    #     else:
    #         new_cat = st.text_input("Введите новую категорию")
    #         if st.button("Добавить категорию"):
    #             if new_cat and type_id:
    #                 conn = app.get_connection()
    #                 cur = conn.cursor()
    #                 cur.execute(
    #                     "INSERT INTO categories (operation_type_id, name) VALUES (?, ?)",
    #                     (type_id, new_cat)
    #                 )
    #                 conn.commit()
    #                 conn.close()
    #                 st.success(f"Категория '{new_cat}' добавлена!")
    #                 # 🔁 обновляем список после добавления
    #                 categories_df = app.get_categories(type_id)
    #                 category_names = categories_df['name'].tolist()
    #                 category_id = int(categories_df.loc[categories_df['name'] == new_cat, 'id_categories'].values[0])
    #             else:
    #                 st.error("Сначала выберите тип операции и введите название категории!")
    #
    #     # --- Подкатегория ---
    #     st.subheader("Подкатегория")
    #     subcats_df = app.get_subcategories(category_id) if category_id else pd.DataFrame(
    #         columns=['id_subcategories', 'name'])
    #     subcat_names = subcats_df['name'].tolist()
    #
    #     selected_subcat = st.selectbox("Выберите подкатегорию", subcat_names + ["➕ Добавить новую"])
    #     subcat_id = None
    #
    #     if selected_subcat == "➕ Добавить новую":
    #         new_subcat = st.text_input("Введите новую подкатегорию")
    #         if st.button("Добавить подкатегорию"):
    #             if new_subcat and category_id:
    #                 conn = app.get_connection()
    #                 cur = conn.cursor()
    #                 cur.execute(
    #                     "INSERT INTO subcategories (category_id, name) VALUES (?, ?)",
    #                     (category_id, new_subcat)
    #                 )
    #                 conn.commit()
    #                 conn.close()
    #                 st.success(f"Подкатегория '{new_subcat}' добавлена!")
    #                 subcats_df = app.get_subcategories(category_id)
    #                 subcat_id = int(subcats_df.loc[subcats_df['name'] == new_subcat, 'id_subcategories'].values[0])
    #             else:
    #                 st.error("Сначала выберите категорию и введите название подкатегории!")
    #     elif selected_subcat != "➕ Добавить новую" and not subcats_df.empty:
    #         subcat_id = int(subcats_df.loc[subcats_df['name'] == selected_subcat, 'id_subcategories'].values[0])
    #
    #     # --- Группа ---
    #     st.subheader("Группа")
    #     conn = app.get_connection()
    #     groups_df = pd.read_sql("SELECT id_groups, name FROM groups WHERE subcategory_id = ?", conn,
    #                             params=(subcat_id,)) if subcat_id else pd.DataFrame(columns=['id_groups', 'name'])
    #     conn.close()
    #     group_names = groups_df['name'].tolist()
    #
    #     selected_group = st.selectbox("Выберите группу", group_names + ["➕ Добавить новую"])
    #     group_id = None
    #
    #     if selected_group == "➕ Добавить новую":
    #         new_group = st.text_input("Введите новую группу")
    #         if st.button("Добавить группу"):
    #             if new_group and subcat_id:
    #                 conn = app.get_connection()
    #                 cur = conn.cursor()
    #                 cur.execute(
    #                     "INSERT INTO groups (subcategory_id, name) VALUES (?, ?)",
    #                     (subcat_id, new_group)
    #                 )
    #                 conn.commit()
    #                 conn.close()
    #                 st.success(f"Группа '{new_group}' добавлена!")
    #                 conn = app.get_connection()
    #                 groups_df = pd.read_sql("SELECT id_groups, name FROM groups WHERE subcategory_id = ?", conn,
    #                                         params=(subcat_id,))
    #                 conn.close()
    #                 group_id = int(groups_df.loc[groups_df['name'] == new_group, 'id_groups'].values[0])
    #             else:
    #                 st.error("Сначала выберите подкатегорию и введите название группы!")
    #     elif selected_group != "➕ Добавить новую" and not groups_df.empty:
    #         group_id = int(groups_df.loc[groups_df['name'] == selected_group, 'id_groups'].values[0])
    #
    #     # --- Подгруппа ---
    #     st.subheader("Подгруппа")
    #     conn = app.get_connection()
    #     subgroups_df = pd.read_sql("SELECT id_subgroups, name FROM subgroups WHERE group_id = ?", conn,
    #                                params=(group_id,)) if group_id else pd.DataFrame(columns=['id_subgroups', 'name'])
    #     conn.close()
    #     subgroup_names = subgroups_df['name'].tolist()
    #
    #     selected_subgroup = st.selectbox("Выберите подгруппу", subgroup_names + ["➕ Добавить новую"])
    #     subgroup_id = None
    #
    #     if selected_subgroup == "➕ Добавить новую":
    #         new_subgroup = st.text_input("Введите новую подгруппу")
    #         if st.button("Добавить подгруппу"):
    #             if new_subgroup and group_id:
    #                 conn = app.get_connection()
    #                 cur = conn.cursor()
    #                 cur.execute(
    #                     "INSERT INTO subgroups (group_id, name) VALUES (?, ?)",
    #                     (group_id, new_subgroup)
    #                 )
    #                 conn.commit()
    #                 conn.close()
    #                 st.success(f"Подгруппа '{new_subgroup}' добавлена!")
    #                 conn = app.get_connection()
    #                 subgroups_df = pd.read_sql("SELECT id_subgroups, name FROM subgroups WHERE group_id = ?", conn,
    #                                            params=(group_id,))
    #                 conn.close()
    #                 subgroup_id = int(subgroups_df.loc[subgroups_df['name'] == new_subgroup, 'id_subgroups'].values[0])
    #             else:
    #                 st.error("Сначала выберите группу и введите название подгруппы!")
    #     elif selected_subgroup != "➕ Добавить новую" and not subgroups_df.empty:
    #         subgroup_id = int(subgroups_df.loc[subgroups_df['name'] == selected_subgroup, 'id_subgroups'].values[0])
    #
    #     # --- Комментарий ---
    #     comment = st.text_area("Комментарий")
    #
    #     # --- Кнопка сохранить ---
    #     if st.button("💾 Сохранить операцию"):
    #         success = app.add_operation(
    #             (
    #                 operation_date, type_id, amount, category_id,
    #                 subcat_id, group_id, subgroup_id, comment
    #             )
    #         )
    #         if success:
    #             st.success("Операция успешно добавлена!")

if __name__ == "__main__":
    main()
