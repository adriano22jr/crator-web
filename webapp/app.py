import flask, requests, app_config

app = flask.Flask(__name__, template_folder = "template_files", static_folder = "static_files")

@app.route('/', methods = ["GET", "POST"])
def index():
    return flask.render_template('index.html', test_value = app_config.TEST_SECRET)


if __name__ == "__main__":
    app.run(port = 8080, debug = True)