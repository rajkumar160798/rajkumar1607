# ==========================================================
# PHASE 1 — Fix Link Inventory Table (cav_link_value_inventory)
# ==========================================================
# 1. Build a session baseline table "s" using Adobe NGX.
# 2. Exclude impersonation events, test URLs, and test PS names.
# 3. Include only sessions where post_evar45 or post_evar71 have valid values.
# 4. Join Adobe events and compute:
#    - post_evar41 (link)
#    - session_cnt (distinct sessions)
#    - hit_cnt (total hits)
# 5. Remove LIMIT.
# 6. Apply filter: session_cnt >= 3000
# 7. Add row_number() ranking by session_cnt desc.
# 8. Create or replace table cav_link_value_inventory.

# ==========================================================
# PHASE 2 — Fix Member Base Table (cav_link_member_base)
# ==========================================================
# 1. Use v_enriched_membership, A1A controls, and ISM controls.
# 2. Add missing columns:
#    - date(a1a.start_date) as a1a_start
#    - date(a1a.end_date) as a1a_end
#    - date(ism.start_date) as ism_start
#    - date(ism.end_date) as ism_end
# 3. Categorize lob_ind_v_group based on lob_cd, cust_seg_cd, cust_subseg_cd.
# 4. Compute member_status using October 15, 2025 rules:
#    - ‘01 active’
#    - ‘02 pending’
#    - ‘03 terminated’
# 5. Remove "#NAME?" errors.
# 6. Use row_number() partitioned by indiv_anlytcs_id ordered by member_status
#    so priority is active → pending → terminated.
# 7. Final SELECT must be:
#      SELECT * FROM b WHERE r = 1.
# 8. Create or replace table cav_link_member_base.

# ==========================================================
# PHASE 3 — Complete Visit Detail Table (cav_link_visit_detail)
# ==========================================================
# 1. Join the visit table (visit_hx) to the member base.
# 2. Join Adobe NGX using session_id and event time partition.
# 3. Join cav_link_value_inventory to attach post_evar41 tags.
# 4. ADD THE CALL HX COMPONENT (missing in user code):
#    - Join call_hx on indiv_anlytcs_sbscrbr_id = subscriber_id.
#    - Include only calls where call_timestamp is within 48 hours of visit.
#    - call_timestamp BETWEEN visit_start AND visit_start + 48 hours.
# 5. Add fields:
#    - call_count (count distinct calls)
#    - call_after_indicator (1 if call exists within 48h else 0).
# 6. Parse timestamp correctly to UTC.
# 7. Create or replace table cav_link_visit_detail.

# ==========================================================
# PHASE 4 — Build Final Summary Table (cav_link_call_summary)
# ==========================================================
# 1. Use cav_link_value_inventory (session_cnt, hit_cnt, rank).
# 2. Use cav_link_visit_detail (call_count).
# 3. Group by year_month and post_evar41.
# 4. Compute:
#    - total_sessions
#    - total_hits
#    - total_calls (within 48 hours)
#    - call_after_rate = total_calls / total_sessions
#    - inherited rank from inventory
# 5. Create or replace table cav_link_call_summary.
