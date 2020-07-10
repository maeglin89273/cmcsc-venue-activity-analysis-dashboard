from datetime import timedelta, datetime
from functools import partial

from bokeh.application.handlers import Handler
from bokeh.layouts import row, gridplot
from bokeh.models import ColumnDataSource, HoverTool
from bokeh.plotting import figure

from data_fetcher import DataFetcher


TOOLTIPS = [
    ('time', '@index{%T}'),
    ('y', '@swimming_pool')
]

FORMATTERS = {'@index': 'datetime'}

INTEREST_COLUMNS = ['swimming_pool']
WEEK_DAYS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
class LiveStreamingHandler(Handler):

    def __init__(self, data_service):
        super().__init__()
        self.data_service = data_service



    def build_live_figure(self, cds):
        live_figure = figure(width_policy='max',
                             height_policy='max',
                             tools='pan,wheel_zoom',
                             active_scroll='wheel_zoom',
                             title='Live',
                             x_axis_type='datetime')

        live_figure.line(x='index', y='swimming_pool', source=cds, line_color='green')
        hover_tool = HoverTool(tooltips=TOOLTIPS,
                               formatters=FORMATTERS,
                               mode='vline')
        live_figure.add_tools(hover_tool)
        return live_figure


    def build_day_figure(self, title, cds, x_range=None):
        fig = figure( tools='pan,wheel_zoom',
                             active_scroll='wheel_zoom',
                             # title=title,
                             x_axis_type='datetime')
        fig.line(x='index', y='swimming_pool', source=cds)
        hover_tool = HoverTool(tooltips=TOOLTIPS,
                               formatters=FORMATTERS,
                               mode='vline')
        fig.add_tools(hover_tool)
        if x_range is not None:
            fig.x_range = x_range
        else:
            x_range = fig.x_range

        return fig, x_range


    def build_week_figures(self, cds_list, start, end):
        figs = []
        x_range = None
        for fig_title, cds in zip(WEEK_DAYS, cds_list):
            fig, x_range = self.build_day_figure(fig_title, cds, x_range)
            figs.append(fig)

        for fig in figs[:-1]:
            fig.xaxis.visible = False
        x_range.start = start
        x_range.end = end
        bounds_start = start.replace(hour=0, second=1)
        bounds_end = start.replace(hour=23, minute=59, second=59)
        x_range.bounds = (bounds_start, bounds_end)
        return figs


    def prepare_week_cds_data(self):

        last_week_data = self.data_service.query_last_week_data()

        week_cds_list = [None] * len(last_week_data)

        sample_week_datetime = None
        for i, df in enumerate(last_week_data):
            df = df[INTEREST_COLUMNS]
            df.index -= timedelta(days=i)
            week_cds_list[i] = ColumnDataSource(df)
            if sample_week_datetime is None and len(df.index) > 0:
                sample_week_datetime = df.index[0]

        if sample_week_datetime is None:
            sample_week_datetime = datetime.now()

        begin_datetime = sample_week_datetime.replace(hour=self.data_service.business_hours.start_hour, minute=0,
                                                      second=0, microsecond=0)
        end_datetime = sample_week_datetime.replace(hour=self.data_service.business_hours.end_hour, minute=0, second=0,
                                                    microsecond=0)
        return week_cds_list, begin_datetime, end_datetime


    def modify_document(self, doc):


        def config_live_figure(x_range, y_max):

            live_figure.y_range.start = 0
            live_figure.y_range.end = y_max
            for fig in day_figures:
                fig.y_range.start = 0
                fig.y_range.end = y_max

            live_figure.x_range.start, live_figure.x_range.end = x_range

        async def fetch_plot_configs():
            y_max = (await self.data_service.data_fetcher.fetch_data()).get_max_number_of_people('swimming_pool')
            time_list = live_cds.data['index']

            if len(time_list) > 0:
                x_range = (time_list[0], time_list[-1])
            else:
                x_start = self.data_service.business_hours.today_start_hour_time
                x_range = (x_start, x_start + timedelta(minutes=1))

            doc.add_next_tick_callback(partial(config_live_figure, x_range, y_max))


        def stream_live_cds(data):
            live_cds.stream(data)

        def on_data_update(data):
            doc.add_next_tick_callback(partial(stream_live_cds, data[INTEREST_COLUMNS]))

        def on_error(err):
            print('session on error', err)


        today_data = self.data_service.subscribe(doc, on_next=on_data_update, on_error=on_error)
        live_cds = ColumnDataSource(today_data[INTEREST_COLUMNS])


        live_figure = self.build_live_figure(live_cds)
        day_figures = self.build_week_figures(*self.prepare_week_cds_data())
        grid_day_figs = gridplot(day_figures, ncols=1, plot_height=730 // len(day_figures), plot_width=560, toolbar_location=None)


        doc.add_root(row(grid_day_figs, live_figure))
        doc.add_next_tick_callback(fetch_plot_configs)