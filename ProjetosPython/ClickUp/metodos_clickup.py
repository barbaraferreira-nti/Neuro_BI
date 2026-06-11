import requests, pprint, time, os, json
import pandas as pd
from config import Config

class api:
    @staticmethod
    def request_clickup(endpoint, params=None):
        CLICKUP_TOKEN = Config.ClickUp.TOKEN_BARBARA
        BASE_URL = Config.ClickUp.URL

        HEADERS = {
            "Authorization": CLICKUP_TOKEN,
            "Content-Type": "application/json"
        }

        url = f"{BASE_URL}{endpoint}"

        while True:
            response = requests.get(url, headers=HEADERS, params=params)

            if response.status_code == 429:
                time.sleep(10)
                continue

            response.raise_for_status()
            return response.json()

    @staticmethod
    def get_workspaces():
        data = api.request_clickup("/team")
        return data.get("teams", [])
    
    @staticmethod
    def get_spaces(team_id):
        data = api.request_clickup(f"/team/{team_id}/space", params={"archived": "false"})
        return data.get("spaces", [])
    
    @staticmethod
    def get_folders(space_id):
        data = api.request_clickup(f"/space/{space_id}/folder", params={"archived": "false"})
        return data.get("folders", [])
    
    @staticmethod
    def get_lists_from_folder(folder_id):
        data = api.request_clickup(f"/folder/{folder_id}/list", params={"archived": "false"})
        return data.get("lists", [])
    
    @staticmethod
    def get_folderless_lists(space_id):
        data = api.request_clickup(f"/space/{space_id}/list", params={"archived": "false"})
        return data.get("lists", [])
    
    @staticmethod
    def get_tasks_from_list(list_id, params=None):
        all_tasks = []
        page = 0

        while True:
            params_page = params.copy() if params else {}
            params_page["page"] = page

            data = api.request_clickup(f"/list/{list_id}/task", params=params_page)

            tasks = data.get("tasks", [])

            if not tasks:
                break

            all_tasks.extend(tasks)
            page += 1

        return all_tasks

    @staticmethod
    def ms_to_datetime(value):
        if not value:
            return None
        return pd.to_datetime(int(value), unit="ms", utc=True).isoformat()
    
    @staticmethod
    def tratar_tasks(tasks):
        rows_tasks = []

        for task in tasks:
            task_id = task.get("id")
            list_id = task.get("list", {}).get("id")
            space_id = task.get("space", {}).get("id")
            status = task.get("status", {}) or {}
            creator = task.get("creator", {}) or {}
            priority = task.get("priority", {}) or {}
    

            rows_tasks.append({
                "task_id": task_id,
                "task_name": task.get("name"),
                "custom_id": task.get("custom_id"),
                "list_id": list_id,
                "space_id": space_id,
                "creator_id": creator.get("id"),
                "text_content": task.get("text_content"),
                "description": task.get("description"),
                "status_id": status.get("id"),
                "status_name": status.get("status"),
                "priority_id": priority.get("id"),
                "priority": priority.get("priority"),
                "priority_color": priority.get("color"),
                "parent": task.get("parent"),
                "top_level_parent": task.get("top_level_parent"),
                "url": task.get("url"),
                "date_created": api.ms_to_datetime(task.get("date_created")),
                "date_updated": api.ms_to_datetime(task.get("date_updated")),
                "date_closed": api.ms_to_datetime(task.get("date_closed")),
                "date_done": api.ms_to_datetime(task.get("date_done")),
                "due_date": api.ms_to_datetime(task.get("due_date")),
                "start_date": api.ms_to_datetime(task.get("start_date"))
                })

        return pd.DataFrame(rows_tasks)
    
    @staticmethod
    def get_user_detail(team_id, user_id):
        data = api.request_clickup(f"/team/{team_id}/user/{user_id}")

        member = data.get("member", {})
        user = member.get("user", {})
        invited_by = member.get("invited_by", {})
        custom_role = user.get("custom_role") or {}

        return {
            "team_id": team_id,
            "user_id": user.get("id"),
            "username": user.get("username"),
            "email": user.get("email"),
            "profile_picture": user.get("profilePicture"),
            "initials": user.get("initials"),
            "color": user.get("color"),
            "role": user.get("role"),
            "custom_role_id": custom_role.get("id"),
            "custom_role_name": custom_role.get("name"),
            "last_active": user.get("last_active"),
            "date_joined": user.get("date_joined"),
            "date_invited": user.get("date_invited"),
            "invited_by_user_id": invited_by.get("id"),
            "invited_by_username": invited_by.get("username"),
            "invited_by_email": invited_by.get("email")
        }
    
    @staticmethod
    def get_users(team_id):

        data = api.request_clickup(f"/team/{team_id}")

        members = data.get("team", {}).get("members", [])

        rows = []

        for member in members:

            user_id = member.get("user", {}).get("id")

            if not user_id:
                continue

            try:
                user_detail = api.get_user_detail(team_id, user_id)
                rows.append(user_detail)
            except Exception as e:
                print(f"Erro ao buscar usuário {user_id} no workspace {team_id}: {e}")
                user = member.get("user", {})
                rows.append({
                    "team_id": team_id,
                    "user_id": user.get("id"),
                    "username": user.get("username"),
                    "email": user.get("email"),
                    "profile_picture": user.get("profilePicture"),
                    "initials": None,
                    "color": user.get("color"),
                    "role": None,
                    "custom_role_id": None,
                    "custom_role_name": None,
                    "last_active": None,
                    "date_joined": None,
                    "date_invited": None,
                    "invited_by_user_id": None,
                    "invited_by_username": None,
                    "invited_by_email": None
                })

        df = pd.DataFrame(rows) 
        if not df.empty:
            df = df.drop_duplicates(subset=["team_id", "user_id"]).reset_index(drop=True)

        return df
    
    @staticmethod
    def get_space_tags(space_id):

        data = api.request_clickup(f"/space/{space_id}/tag")

        tags = data.get("tags", [])

        rows = []

        for tag in tags:

            rows.append({
                "space_id": space_id,
                "tag_name": str(tag.get("name")),
                "tag_fg": str(tag.get("tag_fg")),
                "tag_bg": str(tag.get("tag_bg"))
            })

        df = pd.DataFrame(rows)

        if not df.empty:
            df = (df.drop_duplicates(subset=["space_id", "tag_name"]).reset_index(drop=True))

        return df

    @staticmethod
    def normalizar_task_tags(task):

        task_id = task.get("id")

        tags = task.get("tags", []) or []
        space = task.get("space", {}) or {}

        rows = []

        for tag in tags:

            rows.append({
                "task_id": task_id,
                "space_id": space.get("id"),
                "tag_name": str(tag.get("name"))
            })

        return pd.DataFrame(rows)
    
    @staticmethod
    def normalizar_task_checklist(task):
        task_id = task.get("id")
        checklists = task.get("checklist", []) or []

        rows_checklists = []
        rows_items = []

        for checklist in checklists:
            checklist_id = checklist.get("id")

            rows_checklists.append({
                "checklist_id": checklist_id,
                "task_id": task_id,
                "checklist_name": checklist.get("name"),
                "date_created": checklist.get("date_created"),
                "orderindex": checklist.get("orderindex"),
                "creator_id": checklist.get("creator"),
                "resolved": checklist.get("resolved"),
                "unresolved": checklist.get("unresolved")
            })

            for item in checklist.get("items", []):
                rows_items.append({
                    "checklist_item_id": item.get("id"),
                    "checklist_id": checklist_id,
                    "task_id": task_id,
                    "item_name": item.get("name"),
                    "orderindex": item.get("orderindex"),
                    "resolved": item.get("resolved"),
                    "assignee_id": item.get("assignee"),
                    "parent": item.get("parent"),
                    "date_created": item.get("date_created"),
                    "start_date": item.get("start_date"),
                    "due_date": item.get("due_date")
                })

        return pd.DataFrame(rows_checklists), pd.DataFrame(rows_items)
    
    @staticmethod
    def normalizar_assignees(task):

        task_id = task.get("id")

        assignees = task.get("assignees", [])

        rows = []

        for user in assignees:

            rows.append({
                "task_id": task_id,
                "user_id": user.get("id")
            })

        return pd.DataFrame(rows)
    
    @staticmethod
    def normalizar_watchers(task):

        task_id = task.get("id")

        watchers = task.get("watchers", [])

        rows = []

        for user in watchers:

            rows.append({
                "task_id": task_id,
                "user_id": user.get("id")
            })

        return pd.DataFrame(rows)

    @staticmethod
    def get_task_comments(task_id):
        data = api.request_clickup(f"/task/{task_id}/comment")
        rows = []

        comments = data.get("comments", {})
        

        for comment in comments:
            user = comment.get("user", {}) or {}
            comment_text = "".join(item.get("text", "") for item in comment.get("comment", []))

            rows.append({
            "comment_id": comment.get("id"),
            "comment_text": comment_text,
            "user_id": user.get("id"),
            "date_created": api.ms_to_datetime(comment.get("date")),
            "reply_count": comment.get("reply_count"),
            "task_id": task_id
            })

        return pd.DataFrame(rows)
    
    @staticmethod
    def get_comment_replies(comment_id):
        data = api.request_clickup(f"/comment/{comment_id}/reply")
        rows = []

        replies = data.get("comments", [])

        for reply in replies:
            user = reply.get("user", {}) or {}
            reply_text = "".join(item.get("text", "") for item in reply.get("comment", []))

            rows.append({
                "reply_id": reply.get("id"),
                "parent_comment_id": comment_id,
                "reply_text": reply_text,
                "user_id": user.get("id"),
                "resolved": reply.get("resolved"),
                "date_created": api.ms_to_datetime(reply.get("date")),
                "reply_count": reply.get("reply_count")
            })

        return pd.DataFrame(rows)
    
    @staticmethod
    def get_list_custom_fields(list_id):
        data = api.request_clickup(f'/list/{list_id}/field')

        fields = data.get("fields", []) or []
        rows_fields = []
        rows_options = []

        for field in fields:

            field_id = field.get("id")

            rows_fields.append({
                "field_id": field_id,
                "list_id": list_id,
                "field_name": field.get("name"),
                "type": field.get("type"),
                "date_created": api.ms_to_datetime(field.get("date_created")),
                "hide_from_guests": field.get("hide_from_guests"),
                "required": field.get("required")
            })

            type_config = field.get("type_config", {}) or {}

            options = type_config.get("options", [])

            for option in options:

                rows_options.append({
                    "option_id": option.get("id"),
                    "field_id": field_id,
                    "list_id": list_id,
                    "option_name": option.get("name"),
                    "color": option.get("color"),
                    "orderindex": option.get("orderindex")
                })

        df_fields = pd.DataFrame(rows_fields)
        df_options = pd.DataFrame(rows_options)

        return df_fields, df_options

    @staticmethod
    def normalizar_task_custom_fields(task):
        task_id = task.get("id")
        fields = task.get("custom_fields", []) or []

        rows = []

        for field in fields:
            field_id = field.get("id")
            field_type = field.get("type")

            value = field.get("value")

            field_value_date = None

            if field_type == "date" and value:
                field_value_date = api.ms_to_datetime(value)

            rows.append({
                "task_id": task_id,
                "field_id": field_id,
                "field_value": value,
                "field_value_text": (
                    json.dumps(value, ensure_ascii=False)
                    if isinstance(value, (dict, list))
                    else str(value)
                    if value is not None
                    else None
                ),
                "field_value_date": field_value_date
            })

        return pd.DataFrame(rows)
    
    @staticmethod
    def normalizar_task_attachments(task):
        task_id = task.get("id")
        attachments = task.get("attachments", []) or []

        rows = []

        for attachment in attachments:
            user = attachment.get("user", {}) or {}

            rows.append({
                "attachment_id": attachment.get("id"),
                "task_id": task_id,
                "user_id": user.get("id"),

                "title": attachment.get("title"),
                "extension": attachment.get("extension"),
                "mimetype": attachment.get("mimetype"),

                "type": attachment.get("type"),
                "source": attachment.get("source"),
                "version": attachment.get("version"),

                "date_created": api.ms_to_datetime(attachment.get("date")),

                "size_bytes": attachment.get("size"),
                "total_comments": attachment.get("total_comments"),
                "resolved_comments": attachment.get("resolved_comments"),

                "hidden": attachment.get("hidden"),
                "deleted": attachment.get("deleted"),
                "is_folder": attachment.get("is_folder"),

                "thumbnail_small": attachment.get("thumbnail_small"),
                "thumbnail_medium": attachment.get("thumbnail_medium"),
                "thumbnail_large": attachment.get("thumbnail_large"),

                "url": attachment.get("url"),
                "url_w_query": attachment.get("url_w_query"),
                "url_w_host": attachment.get("url_w_host"),

                "parent_id": attachment.get("parent_id"),
                "workspace_id": attachment.get("workspace_id")
            })

        return pd.DataFrame(rows)

    @staticmethod
    def get_space_status(team_id):
        data = api.request_clickup(f"/team/{team_id}/space")
        spaces = data.get("spaces", []) or []
        rows = []

        for space in spaces:
            space_id = space.get("id")
            statuses = space.get("statuses", []) or []

            for status in statuses:
                rows.append({
                    "space_id": space_id,
                    "status_name": status.get("status"),
                    "status_type": status.get("type"),
                    "status_color": status.get("color"),
                    "orderindex": status.get("orderindex"),
                })

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.drop_duplicates(subset=["space_id", "status_name"]).reset_index(drop=True)

        return df

