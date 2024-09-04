import datetime
import logging
import json
import ckan.plugins.toolkit as tk
import ckan.lib.navl.dictization_functions as df
from ckan.model.user import User
from ckanext.requestdata.logic import schema
from ckanext.requestdata.model import ckanextRequestdata, \
    ckanextUserNotification, ckanextMaintainers, ckanextRequestDataCounters
from ckanext.requestdata import helpers
from ckanext.requestdata.view_helper import process_extras_fields
from ckanext.requestdata.views.admin import __find_packages

NotAuthorized = tk.NotAuthorized
ValidationError = tk.ValidationError
NotFound = tk.ObjectNotFound

_check_access = tk.check_access
__get_action = tk.get_action

log = logging.getLogger(__name__)


def request_create(context, data_dict):
    '''Create new request data.

    :param sender_name: The name of the sender who request data.
    :type sender_name: string

    :param organization: The sender's organization.
    :type organization: string

    :param email_address: The sender's email_address.
    :type email_address: string

    :param message_content: The content of the message.
    :type message_content: string

    :param package_id: The id of the package the data belongs to.
    :type package_id: string

    :returns: the newly created request data
    :rtype: dictionary

    '''

    _check_access('requestdata_request_create', context, data_dict)

    data, errors = df.validate(data_dict, schema.request_create_schema(),
                               context)

    if errors:
        raise ValidationError(errors)

    sender_user_id = User.get(context['user']).id

    sender_name = data.get('sender_name')
    organization = data.get('organization')
    email_address = data.get('email_address')
    message_content = data.get('message_content')
    package_id = data.get('package_id')

    sender_orgs = helpers.get_orgs_for_user(sender_user_id)
    try:
        sender_org = __get_action('organization_show')(context, {'id': data.get('sender_organization_id')})
    except NotFound:
        sender_org = None

    package = __get_action('package_show')(context, {'id': package_id})

    extras = process_extras_fields(data, sender_orgs, sender_org, package)

    if package.get('is_requestdata_type'):
        if package.get('maintainer'):
            maintainers = package['maintainer'].split(',')
        else:
            maintainers = None

        data = {
            'sender_name': sender_name,
            'sender_user_id': sender_user_id,
            'organization': organization,
            'email_address': email_address,
            'message_content': message_content,
            'extras': extras,
            'package_id': package_id
        }

        requestdata = ckanextRequestdata(**data)
        requestdata.save()
        maintainers_list = []
        is_hdx = helpers.is_hdx_portal()

        if maintainers:
            for id in maintainers:
                try:
                    if is_hdx:
                        main_ids = __get_action('user_show')(context, {'id': id})
                        user = User.get(main_ids['id'])
                    else:
                        user = User.get(id)
                    data = ckanextMaintainers()
                    data.maintainer_id = user.id
                    data.request_data_id = requestdata.id
                    data.email = user.email
                    maintainers_list.append(data)
                except NotFound:
                    pass
        out = ckanextMaintainers.insert_all(maintainers_list, requestdata.id)
        return out
    else:
        raise ValidationError('Dataset is not metadata only type')


@tk.side_effect_free
def request_show(context, data_dict):
    '''Return the metadata of a requestdata.

    :param id: The id of a requestdata.
    :type id: string

    :rtype: dictionary

    '''

    data, errors = df.validate(data_dict, schema.request_show_schema(), context)
    if errors:
        raise ValidationError(errors)
    _check_access('requestdata_request_show', context, data_dict)
    id = data.get('id')
    requestdata = ckanextRequestdata.get(id=id)
    if requestdata is None:
        raise NotFound('Request with provided \'id\' cannot be found')
    out = requestdata.as_dict()
    return out


@tk.side_effect_free
def request_list_for_sysadmin(context, data_dict):
    '''Returns a list of all requests.

    :rtype: list of dictionaries

    '''

    _check_access('hdx_request_data_admin_list', context, data_dict)

    requests = ckanextRequestdata.search(order='created_at desc')

    out = []
    package_ids = []

    for item in requests:
        item_dict = item.as_dict()
        extras = item_dict.get('extras', None)
        if extras is not None:
            extras_dict = json.loads(extras)
            item_dict["country"] = extras_dict.get('country','NA')
            item_dict["organization_id"] = extras_dict.get('organization_id','NA')
            item_dict["organization_name"] = extras_dict.get('organization_name','NA')
            item_dict["organization_member"] = extras_dict.get('organization_member','NA')
            item_dict["organization_type"] = extras_dict.get('organization_type','NA')
            item_dict["intend"] = extras_dict.get('intend','NA')
        else:
            item_dict["country"] = 'NA'
            item_dict["organization_id"] = 'NA'
            item_dict["organization_name"] = 'NA'
            item_dict["organization_member"] = 'NA'
            item_dict["organization_type"] = 'NA'
            item_dict["intend"] = 'NA'

        package_ids.append(item_dict.get('package_id'))
        out.append(item_dict)

    try:
        if data_dict.get('include_pkg_org'):
            package_ids = list(set(package_ids))
            search_result = __find_packages(package_ids)
            pkg_dict = {}
            for pkg in search_result.get('results', []):
                pkg_org = pkg.get('organization')
                pkg_dict[pkg.get('id')] = pkg
                #     'pkg_organization_id': pkg_org.get('id'),
                #     'pkg_organization_name': pkg_org.get('name'),
                #     'pkg_organization_title': pkg_org.get('title')
                # }
            for item in out:
                pkg_id = item.get('package_id')
                if pkg_id in pkg_dict:
                    item['pkg_organization_id'] = pkg_dict[pkg_id].get('organization').get('id')
                    item['pkg_organization_name'] = pkg_dict[pkg_id].get('organization').get('name')
                    item['pkg_organization_title'] = pkg_dict[pkg_id].get('organization').get('title')
                    item['dataset_state'] = pkg_dict[pkg_id].get('state')
                    item['is_requestdata_type'] = pkg_dict[pkg_id].get('is_requestdata_type')
                    item['archived'] = pkg_dict[pkg_id].get('archived')
                else:
                    item['pkg_organization_id'] = 'NA'
                    item['pkg_organization_name'] = 'NA'
                    item['pkg_organization_title'] = 'NA'
                    item['dataset_state'] = 'deleted'
                    item['is_requestdata_type'] = False
                    item['archived'] = False

    except Exception as e:
        log.error(e)
        # for item in out:
        #     try:
        #         pkg = __get_action('package_show')(context, {'id': item.get('package_id')})
        #         pkg_org = pkg.get('organization')
        #         item['pkg_organization_id'] = pkg_org.get('id')
        #         item['pkg_organization_name'] = pkg_org.get('name')
        #         item['pkg_organization_title'] = pkg_org.get('title')
        #     except NotFound:
        #         item['pkg_organization_id'] = None
        #         item['pkg_organization_name'] = None
        #         item['pkg_organization_title'] = None
    return out


@tk.side_effect_free
def request_list_for_organization(context, data_dict):
    '''Returns a list of requests for specified organization.

    :param org_id: The organization id.
    :type org_id: string

    :rtype: list of dictionaries

    '''

    data, errors = df.validate(data_dict, schema.request_list_for_organization_schema(), context)

    if errors:
        raise ValidationError(errors)

    _check_access('requestdata_request_list_for_organization', context, data_dict)

    org_id = data.get('org_id')
    org = __get_action('organization_show')(context, {'id': org_id})

    data_dict = {
        'fq': 'organization:{} extras_is_requestdata_type:true'.format(org['name']),
        'rows': 1000000
    }
    packages = __get_action('package_search')(context, data_dict)
    id_to_map = {package['id']:package for package in packages['results']}
    requests = ckanextRequestdata.get_by_package_ids(id_to_map.keys())
    total_requests = []

    for item in requests:
        request_dict = item.as_dict()
        request_dict['package_dict'] = id_to_map[item.package_id]
        total_requests.append(request_dict)

    return total_requests


@tk.side_effect_free
def request_list_for_current_user(context, data_dict):
    '''Returns a list of requests.

    :param id: The id of a requestdata.
    :type id: string

    :rtype: list of dictionaries

    '''

    _check_access('requestdata_request_list_for_current_user',
                 context, data_dict)

    user_id = context['auth_user_obj'].id
    data_dict = {
        'fq': 'maintainer:{} extras_is_requestdata_type:true'.format(user_id),
        'rows': 1000000
    }
    packages = __get_action('package_search')(context, data_dict)
    id_to_map = {package['id']: package for package in packages['results']}
    requests = ckanextRequestdata.get_by_package_ids(id_to_map.keys())
    total_requests = []

    for item in requests:
        request_dict = item.as_dict()
        request_dict['package_dict'] = id_to_map[item.package_id]
        total_requests.append(request_dict)

    return total_requests


def request_patch(context, data_dict):
    '''Patch a request.

    :param id: The id of a request.
    :type id: string

    :returns: A patched request
    :rtype: dictionary

    '''

    request_patch_schema = schema.request_patch_schema()
    fields = list(request_patch_schema.keys())

    # Exclude fields from the schema that are not in data_dict
    for field in fields:
        if field not in data_dict.keys() and (field != 'id' and field != 'package_id'):
            request_patch_schema.pop(field)

    data, errors = df.validate(data_dict, request_patch_schema, context)

    if errors:
        raise ValidationError(errors)

    _check_access('requestdata_request_patch', context, data_dict)

    id = data.get('id')
    package_id = data.get('package_id')

    payload = {
        'id': id,
        'package_id': package_id
    }

    request = ckanextRequestdata.get(**payload)

    if request is None:
        raise NotFound

    request_patch_schema.pop('id')
    request_patch_schema.pop('package_id')

    fields = request_patch_schema.keys()

    for field in fields:
        setattr(request, field, data.get(field))

    request.modified_at = datetime.datetime.now()

    request.save()

    out = request.as_dict()

    return out


def notification_create(context, data_dict):
    '''Create new notification data.

    :param package_id: The id of the package the data belongs to.
    :type package_id: string

    :returns: the newly created notification data
    :rtype: dictionary

    '''
    not_seen = False
    notifications = []
    maintainers = data_dict['users']
    for m in maintainers:
        data = {
            'package_maintainer_id': m['id'],
            'seen': not_seen
        }
        user_notification = ckanextUserNotification(**data)
        user_exist = ckanextUserNotification.get(package_maintainer_id=m['id'])
        if user_exist is None:
            user_notification.save()
            notifications.append(user_notification)
        else:
            user_exist.seen = not_seen
            user_exist.commit()
            notifications.append(user_exist)
    return notifications


@tk.side_effect_free
def notification_for_current_user(context, data_dict):
    '''Returns a notification for logged in user

    :rtype: boolean

    '''

    model = context['model']
    user_id = model.User.get(context['user']).id
    notification = ckanextUserNotification.get(package_maintainer_id=user_id)
    if notification is None:
        # do not display notification
        return True
    else:
        is_notified = notification.seen
        return is_notified


@tk.side_effect_free
def notification_change(context, data_dict):
    '''
        Change the notification status to seen
    :param context:

    :param user_id: The id of logged in user
    :type String

    :return:
    '''

    data, errors = df.validate(data_dict,
                               schema.notification_change_schema(),
                               context)
    if errors:
        raise ValidationError(errors)

    user_id = data.get('user_id')
    notification = ckanextUserNotification.get(package_maintainer_id=user_id)
    if notification is not None:
        notification.seen = True
        notification.commit()
        return notification


def increment_request_data_counters(context, data_dict):
    '''
       Increment the counter for the requested data depending on the flag

       :param package_id: The id of the package the data belongs to.
       :type package_id: string

       :param flag: The flag that indicates which counter to increment
       :type String

       :return:
     '''
    data, errors = df.validate(data_dict,
                               schema.increment_request_counters_schema(),
                               context)
    if errors:
        raise ValidationError(errors)

    flag = data.get('flag')
    package_id = data.get('package_id')
    package = __get_action('package_show')(context, {'id': package_id})
    if package.get('is_requestdata_type') or data_dict.get('shared_publicly'):
        data = {
            'package_id': package_id,
            'org_id': package['owner_org']
        }

        data_request = ckanextRequestDataCounters.get(package_id=package_id)
        if data_request is None:
            new_request = ckanextRequestDataCounters(**data)
            new_request.requests = 1
            new_request.save()
            return new_request
        else:
            if flag == 'request':
                data_request.requests += 1
            elif flag == 'replied':
                data_request.replied += 1
            elif flag == 'declined':
                data_request.declined += 1
            elif flag == 'shared':
                data_request.shared += 1
            elif flag == 'shared and replied':
                data_request.shared += 1
                data_request.replied += 1

            data_request.save()
            return data_request
    else:
        raise ValidationError('Dataset is not metadata only type')


@tk.side_effect_free
def request_data_counters_get(context, data_dict):
    '''
        Returns a counters for particular request data

       :param package_id: The id of the package the request belongs to.
       :type package_id: string

     '''

    package_id = data_dict['package_id']
    counters = ckanextRequestDataCounters.get(package_id=package_id)
    return counters


@tk.side_effect_free
def request_data_counters_get_all(context, data_dict):
    '''
        Returns a counters for particular request data

       :param package_id: The id of the package the request belongs to.
       :type package_id: string

     '''

    counters = ckanextRequestDataCounters.search()
    return counters


@tk.side_effect_free
def request_data_counters_get_by_org(context, data_dict):
    '''
        Return counters for requests that belong to particular organization

       :param org_id: The id of the organizatiion the request belongs to.
       :type org_id: string

     '''

    data = {'org_id': data_dict['org_id']}

    counters = ckanextRequestDataCounters.search_by_organization(**data)

    return counters


def request_update(context, data_dict):
    pass


def request_delete(context, data_dict):
    id = data_dict.get('id')
    requestdata = ckanextRequestdata.get(id=id)
    if requestdata is None:
        raise NotFound('Request with provided \'id\' cannot be found')

    session = context['session']
    requestdata.delete()

    if not context.get('defer_commit'):
        session.commit()
    else:
        session.flush()


def maintainer_delete(context, data_dict):
    maintainer = ckanextMaintainers.get(id=data_dict.get('id'))

    if maintainer is None:
        raise NotFound('Maintainer with provided \'id\' cannot be found')

    session = context['session']
    maintainer.delete()
    if not context.get('defer_commit'):
        session.commit()
    else:
        session.flush()


def counter_delete(context, data_dict):
    counter = ckanextRequestDataCounters.get(id=data_dict.get('id'))

    if counter is None:
        raise NotFound('Counter with provided \'id\' cannot be found')

    session = context['session']
    counter.delete()
    if not context.get('defer_commit'):
        session.commit()
    else:
        session.flush()


def request_delete_by_package_id(context, data_dict):
    _check_access('requestdata_request_delete_by_package_id', context, data_dict)
    package_id = data_dict.get('package_id')
    rq_list = ckanextRequestdata.search(package_id=package_id)
    for rq in rq_list:
        request_id = rq.id
        counters_list = ckanextRequestDataCounters.filter(package_id=package_id).all()
        for c in counters_list:
            counter_delete(context, {'id': c.id})

        maintainers_list = ckanextMaintainers.search(request_data_id=request_id)
        for maintainer in maintainers_list:
            maintainer_delete(context, {'id': maintainer.id})

        request_delete(context, {'id': request_id})

        log.info('Request data was deleted, with id ' + str(rq.id))


def request_archive_by_package_id(context, data_dict):
    _check_access('requestdata_request_archive_by_package_id', context, data_dict)
    package_id = data_dict.get('package_id')
    requests = ckanextRequestdata.get_pending_requests(package_id=package_id)

    for request in requests:
        if request.state == 'new':
            open_data_dict = {
                'id': request.id,
                'package_id': package_id,
                'state': 'open',
                'data_shared': True,
            }
            request_patch(context, open_data_dict)

            replied_data_dict = {
                'package_id': package_id,
                'flag': 'replied',
                'shared_publicly': True
            }
            increment_request_data_counters(context, replied_data_dict)

        archive_data_dict = {
            'id': request.id,
            'package_id': package_id,
            'state': 'archive',
            'data_shared': True,
        }
        request_patch(context, archive_data_dict)

        shared_data_dict = {
            'package_id': package_id,
            'flag': 'shared',
            'shared_publicly': True
        }
        increment_request_data_counters(context, shared_data_dict)

        auto_approval_data_dict = {
            'id': request.id,
            'package_id': package_id
        }
        __get_action('hdx_send_request_data_auto_approval')(context, auto_approval_data_dict)
