import json

from flask import Blueprint

from ckan import model
from ckan.plugins import toolkit as tk
from ckanext.hdx_theme.util.mail import hdx_validate_email as validate_email
from ckanext.requestdata import helpers
from ckanext.requestdata.emailer import send_email
from ckanext.requestdata.view_helper import find_archived_sorting_params, sort_archived, group_requests_by_state, \
    build_id_to_user_map, populate_requests_with_package_title_and_maintainer

NotFound = tk.ObjectNotFound
NotAuthorized = tk.NotAuthorized
ValidationError = tk.ValidationError

asbool = tk.asbool
abort = tk.abort
g = tk.g
_ = tk._
request = tk.request
h = tk.h
_check_access = tk.check_access
__get_action = tk.get_action
config = tk.config
render = tk.render

requestdata = Blueprint(u'requestdata', __name__, url_prefix=u'/user/my_requested_data')


def _get_context():
    return {
        'model': model,
        'session': model.Session,
        'user': g.user,
        'auth_user_obj': g.userobj
    }


def _get_action(action, data_dict):
    return __get_action(action)(_get_context(), data_dict)


def my_requested_data(id):
    '''Handles creating template for 'My Requested Data' page in the
    user's dashboard.

    :param id: The user's id.
    :type id: string

    :returns: template

    '''

    try:
        requests = _get_action('requestdata_request_list_for_current_user',
                               {})
    except NotAuthorized:
        return abort(403, _('Not authorized to see this page.'))

    g.is_myself = id == g.user

    if not g.is_myself:
        return abort(403, _('Not authorized to see this page.'))

    # for item in requests:
    #     try:
    #         package = _get_action('package_show', {'id': item['package_id']})
    #         package_maintainers_ids = package['maintainer'].split(',')
    #         item['title'] = package['title']
    #     except NotFound as e:
    #         # package was not found, possibly deleted
    #         continue
    #     maintainers = []
    #     for i in package_maintainers_ids:
    #         try:
    #             user = _get_action('user_show', {'id': i})
    #             payload = {
    #                 'id': i,
    #                 'fullname': user['fullname']
    #             }
    #             maintainers.append(payload)
    #         except NotFound:
    #             pass
    #     item['maintainers'] = maintainers

    id_to_user_map = build_id_to_user_map(requests)

    populate_requests_with_package_title_and_maintainer(requests, id_to_user_map)

    requests_archive, requests_new, requests_open = group_requests_by_state(requests)

    requests_archive = helpers.group_archived_requests_by_dataset(requests_archive)
    order, reverse, current_order_name = find_archived_sorting_params(request.args.get('order_by'))
    requests_archive = sort_archived(requests_archive, order, reverse)

    extra_vars = {
        'requests_new': requests_new,
        'requests_open': requests_open,
        'requests_archive': requests_archive,
        'current_order_name': current_order_name
    }

    _get_action('requestdata_notification_change', {'user_id': g.userobj.id})

    # data_dict = {
    #     'id': id,
    #     'include_num_followers': True
    # }
    # _setup_template_variables(_get_context(), data_dict)

    return render('requestdata/my_requested_data.html', extra_vars)


# def _setup_template_variables(context, data_dict):
#     g.is_sysadmin = authz.is_sysadmin(g.user)
#     try:
#         user_dict = __get_action('user_show')(context, data_dict)
#     except NotFound:
#         abort(404, _('User not found'))
#     except NotAuthorized:
#         abort(403, _('Not authorized to see this page'))
#
#     g.user_dict = user_dict
#     g.about_formatted = h.render_markdown(user_dict['about'])


def handle_new_request_action(username, request_action):
    '''Handles sending email to the person who created the request, as well
    as updating the state of the request depending on the data sent.

    :param username: The user's name.
    :type username: string

    :param request_action: The current action. Can be either reply or
    reject
    :type request_action: string

    :rtype: json

    '''

    data = request.form.to_dict()

    if request_action == 'reply':
        reply_email = data.get('email')

        try:
            validate_email(reply_email)
        except Exception:
            error = {
                'success': False,
                'error': {
                    'fields': {
                        'email': 'The email you provided is invalid.'
                    }
                }
            }

            return json.dumps(error)

    counters_data_dict = {
        'package_id': data['package_id'],
        'flag': ''
    }
    if 'rejected' in data:
        data['rejected'] = asbool(data['rejected'])
        counters_data_dict['flag'] = 'declined'
    elif 'data_shared' in data:
        counters_data_dict['flag'] = 'shared and replied'
    else:
        counters_data_dict['flag'] = 'replied'

    message_content = data.get('message_content')

    if message_content is None or message_content == '':
        payload = {
            'success': False,
            'error': {
                'message_content': 'Missing value'
            }
        }

        return json.dumps(payload)

    try:
        _get_action('requestdata_request_patch', data)
    except NotAuthorized:
        abort(403, _('Not authorized to use this action.'))
    except ValidationError as e:
        error = {
            'success': False,
            'error': {
                'fields': e.error_dict
            }
        }

        return json.dumps(error)

    to = data['send_to']

    subject = config.get('ckan.site_title') + ': Data request ' + \
              request_action

    file = data.get('file_upload')

    if request_action == 'reply':
        message_content += '<br><br> You can contact the maintainer on ' \
                           'this email address: ' + reply_email

    response = send_email(message_content, to, subject, file=file)

    if response['success'] is False:
        error = {
            'success': False,
            'error': {
                'fields': {
                    'email': response['message']
                }
            }
        }

        return json.dumps(error)

    success = {
        'success': True,
        'message': 'Message was sent successfully'
    }

    action_name = 'requestdata_increment_request_data_counters'
    _get_action(action_name, counters_data_dict)

    return json.dumps(success)


def handle_open_request_action_shared(username):
    return handle_open_request_action(username, u'shared')


def handle_open_request_action_notshared(username):
    return handle_open_request_action(username, u'notshared')


def handle_open_request_action(username, request_action):
    '''Handles updating the state of the request depending on the data
    sent.

    :param username: The user's name.
    :type username: string

    :param request_action: The current action. Can be either shared or
    notshared
    :type request_action: string

    :rtype: json

    '''

    data = request.form.to_dict()
    if 'data_shared' in data:
        data['data_shared'] = asbool(data['data_shared'])
    data_dict = {
        'package_id': data['package_id'],
        'flag': ''
    }
    if 'data_shared' in data:
        if data.get('data_shared'):
            data_dict['flag'] = 'shared'
        else:
            data_dict['flag'] = 'declined'

        action_name = 'requestdata_increment_request_data_counters'
        _get_action(action_name, data_dict)

    try:
        _get_action('requestdata_request_patch', data)
    except NotAuthorized:
        abort(403, _('Not authorized to use this action.'))
    except ValidationError as e:
        error = {
            'success': False,
            'error': {
                'fields': e.error_dict
            }
        }

        return json.dumps(error)

    success = {
        'success': True,
        'message': 'Request\'s state successfully updated.'
    }

    return json.dumps(success)


requestdata.add_url_rule(u'/<id>', view_func=my_requested_data, methods=[u'GET'])

requestdata.add_url_rule(u'/<username>/shared', view_func=handle_open_request_action_shared, methods=[u'GET', u'POST'])
requestdata.add_url_rule(u'/<username>/notshared', view_func=handle_open_request_action_notshared,
                         methods=[u'GET', u'POST'])
