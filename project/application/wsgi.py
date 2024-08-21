from producer_emulator import app as pe_app
from data_loader import app as dl_app
from model_application import app as ma_app


def run():
    from gunicorn.app.base import Application

    class FlaskApplication(Application):
        def __init__(self, app, options=None):
            self.options = options or {}
            self.application = app
            super(FlaskApplication, self).__init__()

        def load_config(self):
            config = dict([(key, value) for key, value in self.options.items()
                           if key in self.cfg.settings and value is not None])
            for key, value in config.items():
                self.cfg.set(key.lower(), value)

        def load(self):
            return self.application

    pe_app.debug = True
    dl_app.debug = True
    ma_app.debug = True

    options_pe = {
        'bind': '0.0.0.0:5000',
        'workers': 3,
        'threads': 2,
        'loglevel': 'debug',
        'accesslog': '-',
        'errorlog': '-'
    }

    options_dl = {
        'bind': '0.0.0.0:5002',
        'workers': 3,
        'threads': 2,
        'loglevel': 'debug',
        'accesslog': '-',
        'errorlog': '-'
    }

    options_ma = {
        'bind': '0.0.0.0:5001',
        'workers': 3,
        'threads': 2,
        'loglevel': 'debug',
        'accesslog': '-',
        'errorlog': '-'
    }

    FlaskApplication(pe_app, options_pe).run()
    FlaskApplication(dl_app, options_dl).run()
    FlaskApplication(ma_app, options_ma).run()


if __name__ == '__main__':
    run()
