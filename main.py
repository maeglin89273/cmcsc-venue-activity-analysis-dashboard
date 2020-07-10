from bokeh.application import Application
from bokeh.server.server import Server

from data_service import DataService, BusinessHours
from live_streaming_handler import LiveStreamingHandler

if __name__ == '__main__':
    data_service = DataService(BusinessHours(start_hour=6, end_hour=22))
    streaming_app = Application(LiveStreamingHandler(data_service))


    server = Server(streaming_app,
                    extra_patterns=[
                        # (r'/data/(.*)', DataReceiver, dict(data_queue=data_queue, queued_data_length=1)),
                        # (r'/sync', ClockSyncHandler, dict(data_queue=data_queue))
                    ],

                    address='0.0.0.0', port=8888, allow_websocket_origin=['localhost:8888', '192.168.0.13:1234'],
                    check_unused_sessions_milliseconds=1000, unused_session_lifetime_milliseconds=5000,
                    debug=False)


    data_service.start(server.io_loop)
    server.run_until_shutdown()
