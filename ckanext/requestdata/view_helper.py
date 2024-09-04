import logging
import json

from ckan import model
from ckan.plugins import toolkit as tk

log = logging.getLogger(__name__)

NotFound = tk.ObjectNotFound
__get_action = tk.get_action
g = tk.g


def _get_action(action, data_dict):
    return __get_action(action)({
        'model': model,
        'session': model.Session,
        'user': g.user or g.author,
        'auth_user_obj': g.userobj
    }, data_dict)


def find_archived_sorting_params(request_param_value):
    reverse = True
    order = 'last_request_created_at'
    current_order_name = 'Most Recent'

    if request_param_value:
        param = request_param_value.split('|')
        order = param[0]
        if 'asc' in order:
            reverse = False
            order = 'title'
            current_order_name = 'Alphabetical (A-Z)'
        elif 'desc' in order:
            reverse = True
            order = 'title'
            current_order_name = 'Alphabetical (Z-A)'
        elif 'most_recent' in order:
            reverse = True
            order = 'last_request_created_at'
        elif 'shared' in order:
            current_order_name = 'Sharing Rate'
        elif 'requests' in order:
            current_order_name = 'Requests Rate'

    return order, reverse, current_order_name


def sort_archived(grouped_requests_archive, order, reverse):
    if order == 'last_request_created_at':
        for dataset in grouped_requests_archive:
            created_at = dataset.get('requests_archived')[0].get('created_at')
            data = {
                'last_request_created_at': created_at
            }
            dataset.update(data)
    grouped_requests_archive = sorted(grouped_requests_archive, key=lambda x: x[order], reverse=reverse)
    return grouped_requests_archive


def group_requests_by_state(requests):
    requests_new = []
    requests_open = []
    requests_archive = []
    for current_request in requests:
        if current_request['state'] == 'new':
            requests_new.append(current_request)
        elif current_request['state'] == 'open':
            requests_open.append(current_request)
        elif current_request['state'] == 'archive':
            requests_archive.append(current_request)
    return requests_archive, requests_new, requests_open


def find_maintainer_metadata(package_maintainer_id):
    '''
    :param package_maintainer_id:
    :type package_maintainer_id: str

    :return:
    :rtype: dict
    '''
    try:
        user_dict = _get_action('user_show', {'id': package_maintainer_id})

        name = user_dict.get('fullname') or user_dict['name']
        user = {
            'id': package_maintainer_id,
            'name': name,
            'username': user_dict['name'],
            'fullname': name,
            'count': 1,
        }
        return user
    except NotFound as e:
        log.error('Couldn\'t get a user metadata for {}'.format(package_maintainer_id))
    return None


def build_id_to_user_map(requests):
    '''
    Builds a map of maintainers (user ids to user dict). The user dict also contains a count of requests for each
    maintainer.

    :param requests: list of requests
    :type requests: list

    :return: dictionary that maps user ids to user dict. The dict also contains the count - number of requests
    '''
    id_to_user_map = {}
    for current_request in requests:
        package = current_request['package_dict']
        package_maintainer_id = package['maintainer']
        user = id_to_user_map.get(package_maintainer_id)
        if not user:
            user = find_maintainer_metadata(package_maintainer_id)
            if user:
                id_to_user_map[package_maintainer_id] = user
        else:
            user['count'] += 1

    return id_to_user_map


def populate_requests_with_package_title_and_maintainer(requests, id_to_user_map):
    for current_request in requests:
        package = current_request['package_dict']
        current_request['title'] = package['title']

        package_maintainer_id = package['maintainer']
        user = id_to_user_map.get(package_maintainer_id)
        current_request['maintainers'] = [user]


def process_extras_fields(data_dict, sender_organizations, organization, dataset_dict=None):
    sender_country = data_dict.get('sender_country')
    sender_organization_id = data_dict.get('sender_organization_id_other') if data_dict.get(
        'sender_organization_id_other') else data_dict.get('sender_organization_id')
    sender_organization_type = data_dict.get('sender_organization_type_other') if data_dict.get(
        'sender_organization_type_other') else data_dict.get('sender_organization_type')
    sender_intend = data_dict.get('sender_intend_other') if data_dict.get('sender_intend_other') else data_dict.get(
        'sender_intend')

    if organization:
        sender_organization_name = organization.get('display_name')
        sender_organization_member = True if (org['id'] == sender_organization_id for org in
                                              sender_organizations) else False
    else:
        sender_organization_name = sender_organization_id
        sender_organization_member = False

    return json.dumps({'country': sender_country,
                       'organization_id': sender_organization_id,
                       'organization_name': sender_organization_name,
                       'organization_member': sender_organization_member,
                       'organization_type': sender_organization_type,
                       'intend': sender_intend,
                       'dataset_dict': dataset_dict})
