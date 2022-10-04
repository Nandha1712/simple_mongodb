from flask import Flask, session, request, jsonify

app = Flask(__name__)

@app.route('/check')
def hellow():
    n_msg = {"msg": "Hello world"}
    return jsonify(n_msg), 200


@app.route('/update/<req_id>/<custom_msg>')
def post_custom_msg(req_id, custom_msg):
    n_msg = {"user": "New", "msg": custom_msg, "req_id": req_id}
    return jsonify(n_msg), 200


# at the bottom of the file, use this to run the app
if __name__ == '__main__':
    app.run(debug=True)