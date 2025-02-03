dataset_list = [
    {
        ## ------- Commercial digital data -------
        # ------- View of Daily_revenue_no_grouping -------
        "url": "https://api.domo.com/v1/datasets/query/execute/31131a4d-3c9a-4e4c-a133-3170b4160452",
        "conditions": {
            "type": lambda x: x.nunique() == 6,
            "Minimum goal": lambda x: x.notna().all() and (x > 0).all(),
            "Sum revenue": lambda x: x.notna().all() and (x > 0).all()
        }
    },
    {
        ## ------- Yesterday in digital - detailed -------
        # ------- s_yesterday_mako_new_2 -------
        "url": "https://api.domo.com/v1/datasets/query/execute/3c971771-55e4-4de6-ace4-e1aaf3e405b5",
        "conditions": {
            "SITE": lambda x: x.nunique() == 5,
            "UNIQUES": lambda x: x.notna().all() and (x > 0).all(),
            "PV": lambda x: x.notna().all() and (x > 0).all(),
        }
    },
    {
        # ------- m_channels_daily_model -------
        "url":"https://api.domo.com/v1/datasets/query/execute/08588f74-d43b-4c41-be95-9df5838f0f33"
    },
    {
        # ------- m_items_pageview_daily -------
        "url": "https://api.domo.com/v1/datasets/query/execute/af0a064c-ce74-4efc-979a-c1d477450429"
    },
    {
        # ------- m_channel_daily_goals -------
        "url":"https://api.domo.com/v1/datasets/query/execute/61f42afa-c723-4e74-8683-f5925ca5d137"
    },
    {
        # ------- m_12Plus_players_new -------
        "url":"https://api.domo.com/v1/datasets/query/execute/3055fbb6-e04f-4386-a524-ee9ed6b4b33e"
    },
    {
        #    -------  mako_n12_player_events -------
        "url":"https://api.domo.com/v1/datasets/query/execute/409b554c-f08a-4602-b1fc-cc58027df9d2"
    },
    {
        ## ------- campaign Daily report -------
        #    -------  v1_player_events_with_ads -------
        "url":"https://api.domo.com/v1/datasets/query/execute/f79f36b0-4108-4d48-80f1-afdf3bd4bd0b"
    },
    {
        #    ------- daily_users_data -------
        "url":"https://api.domo.com/v1/datasets/query/execute/69f1abd3-072f-49d2-b927-e0c24a698a43"
    },
    {
        #    ------- cohort_new_users_daily -------
        "url":"https://api.domo.com/v1/datasets/query/execute/22faae02-8fa7-44c1-80f5-a0547c4d8fa6"
    },
    {
        #    ------- v1_page_view_rt -------
        "url":"https://api.domo.com/v1/datasets/query/execute/07befa93-15c4-4cef-9a05-5cd701ef404c"
    },
    {
        #    ------- m_channels_hourly_model -------
        "url":"https://api.domo.com/v1/datasets/query/execute/6b31e59a-3b8b-4ac4-824c-361cdebdf9e8"
    },
    {
        #    ------- m_pushes -------
        "url":"https://api.domo.com/v1/datasets/query/execute/f79f36b0-4108-4d48-80f1-afdf3bd4bd0b"
    },
        ## ------- mako_daily_report -------
        # s_yesterday_mako_new_2
        # m_channel_daily_goals
        # m_channels_daily_model
        # cohort_new_users_daily
        # m_channels_hourly_model
        # m_pushes
        # mako_n12_player_events
        # m_items_pageview_daily
    {
        #    ------- mako_shorts_player_events -------
        "url": "https://api.domo.com/v1/datasets/query/execute/46d4862e-5d6c-4aa5-9f60-958614778fa6"
    },
    {
        #    ------- mako_daily_qualitative_data -------
        "url": "https://api.domo.com/v1/datasets/query/execute/f0241db7-e0d8-4ddc-a787-efdcfc9e75fe"
    },
    {
        #    ------- m_canonical_most_viewed_item_daily -------
        "url": "https://api.domo.com/v1/datasets/query/execute/f48470d9-073c-4b88-8fef-e642436fea64"
    },
    {
        ## ------- Email to HR -------
        #    ------- workers, clock and vpn -------
        "url": "https://api.domo.com/v1/datasets/query/execute/7e3a8b7f-ec90-4389-9aaf-d797e699df16"
    }


]



