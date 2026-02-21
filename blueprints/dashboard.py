from flask import Blueprint, render_template, current_app
from flask_login import login_required, current_user

dashboard_bp = Blueprint('main', __name__)


@dashboard_bp.route('/')
@login_required
def dashboard():
    return render_template('dashboard.html')
