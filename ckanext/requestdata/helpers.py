import datetime
import itertools
import json
from operator import itemgetter
from six import string_types
import timeago
from paste.deploy.converters import asbool

from ckan import model
from ckan.model.user import User
from ckan.plugins import toolkit as tk
from ckanext.requestdata.model import ckanextRequestDataCounters
from ckanext.hdx_org_group.helpers.static_lists import ORGANIZATION_TYPE_LIST

NotFound = tk.ObjectNotFound
NotAuthorized = tk.NotAuthorized
ValidationError = tk.ValidationError
config = tk.config
g = tk.g
_ = tk._
request = tk.request
get_action = tk.get_action
abort = tk.abort


def _get_context():
    return {
        'model': model,
        'session': model.Session,
        'user': g.user,
        'auth_user_obj': g.userobj
    }


def _get_action(action, data_dict):
    return get_action(action)(_get_context(), data_dict)


def time_ago_from_datetime(date):
    '''Returns a 'time ago' string from an instance of datetime or datetime
    formated string.

    Example: 2 hours ago

    :param date: The parameter which will be formated.
    :type idate: datetime or string

    :rtype: string

    '''

    now = datetime.datetime.now()

    if isinstance(date, datetime.date):
        date = date.strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(date, string_types):
        date = date[:-7]

    return timeago.format(date, now)


def get_package_title(package_id):
    try:
        package = _get_action('package_show', {'id': package_id})
    except NotAuthorized:
        abort(403, _('Not authorized to see this package.'))
    except NotFound:
        abort(403, _('Package not found.'))

    return package['title']


def get_notification():
    '''Returns a boolean which indicates if notification was seen or not

      :rtype: bool

      '''

    notification = _get_action('requestdata_notification_for_current_user', {})
    return notification


def get_request_counters(id):
    '''
        Returns a counters for particular request data

       :param package_id: The id of the package the request belongs to.
       :type package_id: string

     '''

    package_id = id
    data_dict = {'package_id': package_id}
    counters = _get_action('requestdata_request_data_counters_get', data_dict)
    return counters


def convert_id_to_email(ids):
    ids = ids.split(',')
    emails = []

    for id in ids:
        user = User.get(id)

        if user:
            emails.append(user.email)
        else:
            emails.append(id)

    return ','.join(emails)


def group_archived_requests_by_dataset(requests):
    sorted_requests = sorted(requests, key=itemgetter('package_id'))
    grouped_requests = []
    package_ids = []

    for key, group in itertools.groupby(sorted_requests,
                                        key=lambda x: x['package_id']):
        package_ids.append(key)
        requests = list(group)
        data = {
            'package_id': key,
            'title': requests[0]['package_dict'].get('title'),
            'maintainers': requests[0]['package_dict'].get('maintainer'),
            'requests_archived': requests,
        }

        grouped_requests.append(data)
    package_id_to_counters = fetch_counters_for_packages_as_map(package_ids)
    for data in grouped_requests:
        counters_dict = package_id_to_counters[data['package_id']]
        data.update(counters_dict)

    return grouped_requests


def fetch_counters_for_packages_as_map(package_ids):
    package_id_to_counters = {}
    counters = ckanextRequestDataCounters.get_by_package_ids(package_ids)
    for counter in counters:
        package_id_to_counters[counter.package_id] = {
            'shared': counter.shared,
            'requests': counter.requests,
            'replied': counter.replied,
            'declined': counter.declined,
        }
    return package_id_to_counters


def has_query_param(param):
    # Checks if the provided parameter is part of the current URL query params

    params = dict(request.params)

    if param in params:
        return True

    return False


def convert_str_to_json(data):
    try:
        return json.loads(data)
    except Exception:
        return 'string cannot be parsed'


def is_hdx_portal():
    return asbool(config.get('hdx_portal', False))


def is_current_user_a_maintainer(maintainers):
    if g.user and maintainers:
        current_user = _get_action('user_show', {'id': g.user})
        user_id = current_user.get('id')
        user_name = current_user.get('name')

        if user_id in maintainers or user_name in maintainers:
            return True
    return False


def get_orgs_for_user(user_id, include_org_type=False):
    try:
        orgs = _get_action('organization_list_for_user', {'id': user_id})

        if include_org_type:
            query = model.Session.query(model.GroupExtra)
            query = query.filter_by(key='hdx_org_type', state='active')
            org_extras = query.all()

            extras = {}
            for org_extra in org_extras:
                extras[org_extra.group_id] = org_extra.value

            for org in orgs:
                org_id = org.get('id')
                if org_id in extras:
                    org['org_type'] = extras[org_id]

        return orgs
    except Exception:
        return []


def role_in_org(user_id, org_name):
    try:
        org = _get_action('organization_show', {'id': org_name})
    except NotFound:
        return ''

    for user in org.get('users', []):
        if user.get('id') == user_id:
            return user.get('capacity')


def get_org_type_value(org_type_key):
    return next((org_type[0] for org_type in ORGANIZATION_TYPE_LIST if org_type[1] == org_type_key), org_type_key)

