"""클라우드 배포용 WSGI 엔트리포인트"""
from app import create_app, init_db

app = create_app()
init_db(app)

if __name__ == '__main__':
    app.run()
