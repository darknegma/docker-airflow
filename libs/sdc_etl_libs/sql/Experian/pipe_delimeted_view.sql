CREATE OR REPLACE VIEW "MARKETING"."EXPERIAN"."VW_INDIV_HH_LEVEL_DATA_NO_PII" COPY GRANTS AS
(
SELECT
        CASE WHEN PII_ACCESS = TRUE THEN "CLIENT_ID" ELSE sha2("CLIENT_ID", 512) END AS "CLIENT_ID",
        "UE_MATCH_LVL",
        "UE_MATCH_ADD",
        "ZIP_LOCALITY",
        "INCOME_PERCENTILE_NATIONAL",
        "COUNTY_EHI_PERCENTILE",
        "DWELLING_TYPE",
        "CAPE_COUNT_HOUSEHOLDS_HH",
        "CAPE_COUNT_POPULATION_POP",
        "TIME_ZONE",
        "PROPERTY_REALTY_HOME_YEAR_BUILT",
        "CENSUS_2010_BLOCK_GROUP",
        "CENSUS_2010_BLOCK_ID",
        "CENSUS_2010_TRACT",
        "CAPE_AGE_POP_0_9",
        "CAPE_AGE_POP_0_17",
        "CAPE_AGE_POP_0_24",
        "CAPE_AGE_POP_18_20",
        "CAPE_AGE_POP_18_65",
        "CAPE_AGE_POP_21_24",
        "CAPE_AGE_POP_25_34",
        "CAPE_AGE_POP_35_44",
        "CAPE_AGE_POP_45_54",
        "CAPE_AGE_POP_55_64",
        "CAPE_AGE_POP_65_99",
        "CAPE_AGE_POP_75_99",
        "CAPE_AGE_POP_MEDIAN_AGE_OF_ADULTS_18",
        "CAPE_BUILT_HU_MEDIAN_HOUSING_UNIT_AGE",
        "CAPE_CHILD_HH_FEMALE_HOH_FAM_W_PERSONS_LT18",
        "CAPE_CHILD_HH_WITH_PERSONS_LT18",
        "CAPE_CHILD_HH_WITHOUT_PERSONS_LT18",
        "CAPE_COMMUTE_COMMUTER_TRAV_TO_WORK_60_89_MIN",
        "CAPE_COMMUTE_COMMUTER_AVG_TRAV_TIME_TO_WORK",
        "CAPE_COMMUTE_WRKRS_CARPOOLED_TO_WORK",
        "SCSP_ALL0300_AVG",
        "SCSP_ALL1300_AVG",
        "SCSP_ALL2380_AVG",
        "SCSP_ALL2870_AVG",
        "SCSP_ALL2968_AVG",
        "SCSP_ALL5040_AVG",
        "SCSP_ALL5820_AVG",
        "SCSP_ALL6160A_PCT",
        "SCSP_ALL6200A_PCT",
        "SCSP_ALL6280A_PCT",
        "SCSP_ALL7110_AVG",
        "SCSP_ALL7310_AVG",
        "SCSP_ALL7340_AVG",
        "SCSP_ALL9120_AVG",
        "SCSP_AUA0300_AVG",
        "SCSP_AUA0437_AVG",
        "SCSP_AUA2320_AVG",
        "SCSP_AUA2800_AVG",
        "SCSP_AUA5320_AVG",
        "SCSP_AUL0436_AVG",
        "SCSP_AUT0300_AVG",
        "SCSP_BCA5020_AVG",
        "SCSP_BCA5070_AVG",
        "SCSP_BCC0300_AVG",
        "SCSP_BCC0437_AVG",
        "SCSP_BCC3421_AVG",
        "SCSP_BCC3423_AVG",
        "SCSP_BCC3456_AVG",
        "SCSP_BCC5421_AVG",
        "SCSP_BCC5620_AVG",
        "SCSP_BCC6160B_PCT",
        "SCSP_BCC7110_AVG",
        "SCSP_BCC8320_AVG",
        "SCSP_COL2750_AVG",
        "SCSP_COL2758_AVG",
        "SCSP_CRU0300_AVG",
        "SCSP_FIP0437_AVG",
        "SCSP_FIP2350_AVG",
        "SCSP_FIP5320_AVG",
        "SCSP_FIP6200A_PCT",
        "SCSP_HLC2328_AVG",
        "SCSP_HLC5320_AVG",
        "SCSP_ILN0416_AVG",
        "SCSP_ILN5320_AVG",
        "SCSP_ILN7430_AVG",
        "SCSP_ILN8220_AVG",
        "SCSP_IQB9410_AVG",
        "SCSP_IQB9416_AVG",
        "SCSP_IQF9410_AVG",
        "SCSP_IQM9416_AVG",
        "SCSP_IQM9510_AVG",
        "SCSP_IQR9410_AVG",
        "SCSP_IQR9416_AVG",
        "SCSP_IQT9410_AVG",
        "SCSP_IQT9417_AVG",
        "SCSP_MTA0300_AVG",
        "SCSP_MTA1300_AVG",
        "SCSP_MTA2380_AVG",
        "SCSP_MTA4380_AVG",
        "SCSP_MTA5020_AVG",
        "SCSP_MTA6160A_PCT",
        "SCSP_MTA8220_AVG",
        "COUNTY_ECHV_INDEX",
        "COUNTY_ECHV_PERCENTILE",
        "STATE_EHI_INDEX",
        "COUNTY_EHI_INDEX",
        "SCSP_MTF0155_AVG",
        "SCSP_MTF0300_AVG",
        "SCSP_MTF2860_AVG",
        "SCSP_MTF2935_AVG",
        "SCSP_MTF5020_AVG",
        "SCSP_MTF8151_AVG",
        "SCSP_MTS0300_AVG",
        "SCSP_REV0300_AVG",
        "CAPE_EDUC_POP25_BACHELOR_DEGREE",
        "CAPE_EDUC_POP25_GRADUATE_PROF_DEGREE",
        "CAPE_EDUC_POP25_HIGH_SCHOOL_GRAD",
        "CAPE_EDUC_POP25_SOME_COLLEGE",
        "CAPE_EDUC_POP25_MEDIAN_EDUCATION_ATTAINED",
        "CAPE_EMPLOY_POPFEM16_IN_LABOR_FORCE",
        "CORE_BASED_STATISTICAL_AREAS_CBSA",
        "AMS_AMERICAN_ORIGIN",
        "AMS_EUROPEAN_ORIGIN",
        "AMS_ASIAN_ORIGIN",
        "DWELLING_UNIT_SIZE",
        "SCSP_REV5420_AVG",
        "SCSP_REV5740_AVG",
        "STATE_CODE",
        "COUNTY_CODE",
        "SCSP_REV7110_AVG",
        "SCSP_REV7410_AVG",
        "SCSP_REV7430_AVG",
        "SCSP_REV8320_AVG",
        "SCSP_RTA0300_AVG",
        "SCSP_RTA0436_AVG",
        "SCSP_RTA5320_AVG",
        "SCSP_RTR1380_AVG",
        "SCSP_RTR3422_AVG",
        "SCSP_RTR3510_AVG",
        "SCSP_STU0300_AVG",
        "SCSP_STU1100_AVG",
        "SCSP_STU5020_AVG",
        "SCSP_STU5820_AVG",
        "SCSP_UTI0300_AVG",
        "SCSP_UTI4180_AVG",
        "CAPE_ETHNIC_HH_HOH_NONHISP_WHITE_ONLY",
        "CAPE_ETHNIC_POP_ASIAN_ONLY",
        "CAPE_ETHNIC_POP_BLACK_ONLY",
        "CAPE_ETHNIC_POP_BLACK_ONLY_HISP",
        "CAPE_ETHNIC_POP_HISPANIC",
        "CAPE_ETHNIC_POP_WHITE_ONLY",
        "CAPE_ETHNIC_POP_WHITE_ONLY_HISP",
        "CAPE_GENDER_POP_MALE",
        "CAPE_HEAT_OCCHU_UTILITY_GAS_HEAT",
        "CAPE_HHSIZE_HH_AVERAGE_HOUSEHOLD_SIZE",
        "CENSUS_RURAL_URBAN_COUNTY_SIZE_CODE",
        "CAPE_HOMVAL_OOHU_MEDIAN_HOME_VALUE",
        "CAPE_INC_HH_HOUSEHOLD_INCOME_UNDER_10K",
        "CAPE_INC_HH_HOUSEHOLD_INCOME_10_14K",
        "CAPE_INC_HH_HOUSEHOLD_INCOME_15_19K",
        "CAPE_INC_HH_HOUSEHOLD_INCOME_20_24K",
        "CAPE_INC_HH_HOUSEHOLD_INCOME_25_29K",
        "CAPE_INC_HH_HOUSEHOLD_INCOME_30_34K",
        "CAPE_INC_HH_HOUSEHOLD_INCOME_35_39K",
        "CAPE_INC_HH_HOUSEHOLD_INCOME_40_44K",
        "CAPE_INC_HH_HOUSEHOLD_INCOME_45_49K",
        "CAPE_INC_HH_HOUSEHOLD_INCOME_50_59K",
        "CAPE_INC_HH_HOUSEHOLD_INCOME_60_74K",
        "CAPE_INC_HH_HOUSEHOLD_INCOME_75_99K",
        "CAPE_INC_HH_HOUSEHOLD_INCOME_100_124K",
        "CAPE_INC_HH_HOUSEHOLD_INCOME_125_149K",
        "CAPE_INC_HH_HOUSEHOLD_INCOME_150_199K",
        "CAPE_INC_HH_HOUSEHOLD_INCOME_200_249K",
        "CAPE_INC_HH_HOUSEHOLD_INCOME_250_499K",
        "CAPE_INC_HH_HOUSEHOLD_INCOME_500K_OR_MORE",
        "CAPE_INC_HH_MEDIAN_HOUSEHOLD_INCOME",
        "CAPE_INDUS_EMPLD_ACCOMODATION_AND_FOOD_SVCS",
        "CAPE_INDUS_EMPLD_ADMIN_SUPP_AND_WASTE_MGMT",
        "CAPE_INDUS_EMPLD_EDUCATIONAL_SERVICES",
        "CAPE_INDUS_EMPLD_FINANCE_AND_INSURANCE",
        "CAPE_INDUS_EMPLD_HLTH_CARE_SOCIAL_ASSISTANCE",
        "CAPE_INDUS_EMPLD_INFORMATION",
        "CAPE_INDUS_EMPLD_PUBLIC_ADMINISTRATION",
        "CAPE_INDUS_EMPLD_REAL_ESTATE_RENTAL_LEASING",
        "CAPE_INDUS_EMPLD_RETAIL_TRADE",
        "CAPE_INDUS_EMPLD_TRANSPORT_AND_WAREHOUSING",
        "CAPE_INDUS_EMPLD_WHOLESALE_TRADE",
        "CAPE_LANG_HH_SPANISH_SPEAKING",
        "CAPE_MORTG_OOHU_FIRST_MORTGAGE_ONLY",
        "CAPE_MOVE_OCCHU_MEDIAN_LENGTH_OF_RESIDENCE",
        "CAPE_RENT_RNTL_MEDIAN_RENT",
        "CAPE_TENANCY_HU_OCCUPIED",
        "CAPE_TENANCY_OCCHU_OWNER_OCCUPIED",
        "CAPE_TYP_HH_MARRIED_COUPLE_FAMILY",
        "CAPE_TYP_POP_IN_FAMILY_HH",
        "CAPE_TYP_POP_IN_NON_FAMILY_HH",
        "CAPE_TYP_POP_SPOUSE_IN_FAMILY_HH",
        "CAPE_URBAN_POP_URBAN_IN_URBAN_AREAS",
        "CAPE_EDUC_ISPSA",
        "CAPE_EDUC_ISPSA_DECILE",
        "CAPE_INC_FAMILY_INC_STATE_INDEX",
        "CAPE_INC_FAMILY_INC_STATE_DECILE",
        "AMS_NUM_FOREIGN_VERSUS_DOMESTIC_VEHICLES",
        "AMS_NUM_NEWER_MORE_EXPENSIVE_VS_OLDER_LESS_EXP",
        "AMS_NUM_LUXURY_NEAR_LUXURY_VS_MEDIUM_PRICED_VEH",
        "AMS_NUM_NEW_VERSUS_USED_VEHICLES",
        "AMS_NUM_MID_RNG_CARS_VS_SM_PICKUPS_LRG_SUVS_PICKUP",
        "SCSP_FACTOR_SCORE_CREDIT_SEEKERS",
        "SCSP_FACTOR_SCORE_FORECLOSURES",
        "SCSP_FACTOR_SCORE_BAD_VS_GOOD_CREDIT",
        "SCSP_FACTOR_SCORE_NUM_MONTHS_MORT_OPENED",
        "SCSP_FACTOR_SCORE_OPEN_RETAIL_TRADES",
        "SCSP_FACTOR_SCORE_RECENTLY_OPENED_MORTGAGE",
        "SCSP_FACTOR_SCORE_REVOLVE_BANKCARD_BALANCES",
        "SCSP_FACTOR_SCORE_SHORT_TERM_DELINQUENCIES",
        "SCSP_FACTOR_SCORE_STUDENT_LOANS",
        "SCSP_FACTOR_SCORE_INSTALL_AUTO_TRADES_AMT",
        "LONGITUDE",
        "MATCH_LEVEL_FOR_GEO_DATA",
        "LATITUDE",
        "CAPE_HISPANIC_VS_NON_HISPANIC_SCORE",
        "CAPE_FAMILY_HM_OWNERS_VS_NONFAMILY_RENTERS_SCORE",
        "CAPE_OLDER_VS_YOUNGER_AGE_SCORE",
        "CAPE_HIGH_VS_LOW_AFFLUENCE_SCORE",
        "CAPE_AFRICANAMERICAN_VS_NONAFRICAN_AMERICAN_SCORE",
        "CAPE_EMPLOYED_WAGEEARNERS_VS_OTHER_INCOME_SCORE",
        "CAPE_COLLEGESTUDNT_AREAS_VS_NONSTUDNT_AREAS_SCOR",
        "CAPE_OLDER_VS_NEWER_BUILT_HOMES_SCORE",
        "DEMAND_20100_GROCERIES",
        "DEMAND_20130_ALCOHOLIC_BEVERAGES_SERVED",
        "DEMAND_20140_PACKAGED_LIQUOR_WINE_BEER",
        "DEMAND_20150_CIGARS_SMOKERS_ACCESS",
        "DEMAND_20160_DRUGS_HEALTH_AND_BEAUTY_AIDS",
        "DEMAND_20180_SOAPS_DETERGENTS_HH_CLEANERS",
        "DEMAND_20190_PAPER_AND_RELATED_PRODUCTS",
        "DEMAND_20200_MENS_WEAR_AND_ACCESSORIES",
        "DEMAND_20220_WOMENS_JUNIORS_AND_MISSES_WEAR",
        "DEMAND_20240_CHILDRENS_WEAR",
        "DEMAND_20260_FOOTWEAR_INC_ACCESSORIES",
        "DEMAND_20270_SEWING_KNITTING_MATERIALS",
        "DEMAND_20280_CURTAINS_DRAPERIES_BLINDS",
        "DEMAND_20300_MAJOR_HOUSEHOLD_APPLIANCES",
        "DEMAND_20310_SMALL_ELECTRIC_APPLIANCES",
        "DEMAND_20320_TVS_VIDEO_RECORDERS_CAMERAS",
        "DEMAND_20330_AUDIO_EQUIP_ETC",
        "DEMAND_20340_FURNITURE_SLEEP_OUTDOOR",
        "DEMAND_20360_FLOORING_FLOOR_COVERINGS",
        "DEMAND_20370_COMPUTER_HARDWARE_SOFTWARE",
        "DEMAND_20380_KITCHENWARE_HOME_FURNISHINGS",
        "DEMAND_20400_JEWELRY",
        "DEMAND_20420_BOOKS",
        "DEMAND_20440_PHOTOGRAPHIC_EQUIP_AND_SUPPLIES",
        "DEMAND_20460_TOYS_HOBBY_GOODS_GAMES",
        "DEMAND_20490_OPTICAL_GOODS_EYEGLASSES_CONTACTS",
        "DEMAND_20530_SPORTING_GOODS",
        "DEMAND_20580_RVS_CAMPING_ACCESSORIES",
        "DEMAND_20600_HARDWARE_TOOLS_PLUMBING_ELECTRICAL",
        "DEMAND_20620_LAWN_GARDEN_FARM_EQUIP",
        "DEMAND_20640_DIMENSIONAL_LUMBER_BLDG_MATERIALS",
        "DEMAND_20670_PAINT_AND_SUNDRIES",
        "DEMAND_20700_CARS_TRUCKS_MOTORCYCLES",
        "DEMAND_20720_AUTOMOTIVE_FUELS",
        "DEMAND_20730_AUTOMOTIVE_LUBRICANTS",
        "DEMAND_20740_AUTOMOTIVE_TIRES_ACCESSORIES",
        "DEMAND_20780_HOUSEHOLD_FUEL",
        "DEMAND_20800_PETS_PET_FOODS_SUPPLIES",
        "DEMAND_20850_ALL_OTHER_MERCHANDISE",
        "DEMAND_21100_MEALS_BEVERAGES_FOR_CONSUMPTION",
        "DEMAND_21220_MEALS_BEVERAGES_FOR_CATERED_EVENT",
        "DEMAND_441_MOTOR_VEHICLE_PARTS_DEALERS",
        "DEMAND_442_FURNITURE_HOME_FURNISHINGS",
        "DEMAND_443_ELECTRONICS_APPLIANCE_STORES",
        "DEMAND_444_BLDG_MATERIAL_GRDN_EQUIP_SUPPLY_DEALERS",
        "DEMAND_445_FOOD_BEVERAGE_STORES",
        "DEMAND_446_HEALTH_PERSONAL_CARE_STORES",
        "DEMAND_447_GASOLINE_STATIONS",
        "DEMAND_448_CLOTHING_ACCESSORIES",
        "DEMAND_451_SPORTING_GOODS_HOBBY_BOOK_MUSIC",
        "DEMAND_452_GENERAL_MERCHANDISE_STORES",
        "DEMAND_453_MISCELLANEOUS_STORE_RETAILERS",
        "DEMAND_454_NONSTORE_RETAILERS",
        "DEMAND_722_FOOD_SERVICE_DRINKING_PLACES",
        "DEMAND_GAFO_GENERAL_MERCHANDISE_ACCESSORIES",
        "DIGITAL_MOMS",
        "EST_CURRENT_MORTGAGE_AMOUNT",
        "EST_CURRENT_LOAN_TO_VALUE_RATIO",
        "EST_AVAILABLE_EQUITY_LL",
        "EST_CURR_MTHLY_MORT_PAYMT_RNG",
        "NUMBER_OF_ADULTS_IN_LIVING_UNIT",
        "NUMBER_OF_CHILDREN_IN_LIVING_UNIT",
        "LENGTH_OF_RESIDENCE",
        "MAIL_OBJECTOR",
        "HOME_BUSINESS",
        "MAIL_RESPONDER",
        "MOR_BANK_GIFTS_AND_GADGETS_BUYER",
        "MOR_BANK_COLLECT_SPECIAL_FOODS_BUYER",
        "MOR_BANK_BOOK_BUYER",
        "MOR_BANK_GARDENING_FARMING_BUYER",
        "MOR_BANK_CRAFTS_HOBBY_MERCHANDISE_BUYER",
        "MOR_BANK_FEMALE_MERCHANDISE_BUYER",
        "MOR_BANK_MALE_MERCHANDISE_BUYER",
        "MOR_BANK_UPSCALE_MERCHANDISE_BUYER",
        "MOR_BANK_HEALTH_AND_FITNESS_MAGAZINE",
        "MOR_BANK_CULINARY_INTERESTS_MAGAZINE",
        "MOR_BANK_GARDENING_FARMING_MAGAZINE",
        "MOR_BANK_RELIGIOUS_MAGAZINE",
        "MOR_BANK_MALE_SPORTS_MAGAZINE",
        "MOR_BANK_FEMALE_ORIENTED_MAGAZINE",
        "MOR_BANK_FAMILY_GENERAL_MAGAZINE",
        "MOR_BANK_GENERAL_MERCHANDISE_BUYER",
        "MOR_BANK_GENERAL_CONTRIBUTOR",
        "MOR_BANK_HEALTH_INSTITUTION_CONTRIBUTOR",
        "MOR_BANK_POLITICAL_CONTRIBUTOR",
        "MOR_BANK_RELIGIOUS_CONTRIBUTOR",
        "MOR_BANK_OPPORTUNITY_SEEKERS_CONTESTS",
        "MOR_BANK_PHOTOGRAPHY",
        "MOR_BANK_NEWS_AND_FINANCIAL",
        "MOR_BANK_DO_IT_YOURSELFERS",
        "MOR_BANK_ODDS_AND_ENDS",
        "MOR_BANK_MISCELLANEOUS",
        "ESTIMATED_CURRENT_HOME_VALUE",
        "MOR_BANK_DEDUPED_CATEGORY_HIT_COUNT",
        "MORTGAGE_HOME_PURCHASE_MORTGAGE_TERM_IN_MONTHS",
        "MORTGAGE_HOME_PURCHASE_HOME_PURCHASE_DATE",
        "MORTGAGE_HOME_PURCHASE_MORTGAGE_RATE_TYPE",
        "MORTGAGE_HOME_PURCHASE_MORTGAGE_LOAN_TYPE",
        "MORTGAGE_HOME_PURCHASE_TYPE_OF_PURCHASE",
        "DIGITAL_DADS",
        "RECIPIENT_RELIABILITY_CODE",
        "HOMEOWNER_COMBINED_HOMEOWNER_RENTER",
        "MORTGAGE_HOME_PURCHASE_PURCHASE_AMOUNT_RANGES",
        "MORTGAGE_HOME_PURCHASE_MORTGAGE_AMOUNT_RANGES",
        "GREEN_AWARE",
        "ONLINE_OVERALL",
        "RETAIL_OVERALL",
        "HOUSEHOLD_COMPOSITION",
        "BEHAVIORBANK_PRESENCE_OF_CREDIT_CARD",
        "BEHAVIORBANK_INTEREST_IN_CRAFTS",
        "BEHAVIORBANK_INTEREST_IN_GOURMET_COOKING",
        "BEHAVIORBANK_COMPUTERS_PERIPHERALS",
        "BEHAVIORBANK_HI_TECH_OWNER",
        "BEHAVIORBANK_INTERNET_ONLINE_SUBSCRIBER",
        "CHILDREN_AGE_0_3",
        "CHILDREN_AGE_4_6",
        "CHILDREN_AGE_7_9",
        "CHILDREN_AGE_10_12",
        "CHILDREN_AGE_13_15",
        "CHILDREN_AGE_16_18",
        "CHILDREN_PRESENCE_OF_CHILD_0_18",
        "CHILDREN_AGE_0_3_GENDER",
        "CHILDREN_AGE_4_6_GENDER",
        "CHILDREN_AGE_7_9_GENDER",
        "CHILDREN_AGE_10_12_GENDER",
        "CHILDREN_AGE_13_15_GENDER",
        "CHILDREN_AGE_16_18_GENDER",
        "HOUSEHOLD_CONSUMER_EXPENDITURES_APPAREL",
        "HOUSEHOLD_CONSUMER_EXPENDITURES_SHOES",
        "HOUSEHOLD_CONSUMER_EXPENDITURES_ACCESSORIES",
        "HOUSEHOLD_CONSUMER_EXPENDITURES_TOYS",
        "HOUSEHOLD_CONSUMER_EXPENDITURES_FURNITURE",
        "HOUSEHOLD_CONSUMER_EXPENDITURES_KITCHEN",
        "HOUSEHOLD_CONSUMER_EXPENDITURES_TABLETOP_DINE",
        "HOUSEHOLD_CONSUMER_EXPENDITURES_FOOD_BEV",
        "HOUSEHOLD_CONSUMER_EXPENDITURES_HOME_OFFICE",
        "HOUSEHOLD_CONSUMER_EXPENDITURES_HOME_DECOR",
        "HOUSEHOLD_CONSUMER_EXPENDITURES_HOMEDOMESTIC",
        "HOUSEHOLD_CONSUMER_EXPENDITURES_HOME_MAINT",
        "HOUSEHOLD_CONSUMER_EXPENDITURES_OUTDOOR_LIVE",
        "HOUSEHOLD_CONSUMER_EXPENDITURES_LAWN_GARDEN",
        "HOUSEHOLD_CONSUMER_EXPENDITURES_ELECTRON_GADG",
        "HOUSEHOLD_CONSUMER_EXPENDITURES_COMPUTERS",
        "HOUSEHOLD_CONSUMER_EXPENDITURES_PETS",
        "HOUSEHOLD_CONSUMER_EXPENDITURES_HOBBIES_ENTAIN",
        "HOUSEHOLD_CONSUMER_EXPENDITURES_PERSONALHEALTH",
        "HOUSEHOLD_CONSUMER_EXPENDITURES_OUTDOORSFTGOOD",
        "HOUSEHOLD_CONSUMER_EXPENDITURES_OUTDOORHRDGOOD",
        "HOUSEHOLD_CONSUMER_EXPENDITURES_TRAVEL",
        "HOUSEHOLD_CONSUMER_EXPENDITURES_SEASONALPRODS",
        "HOUSEHOLD_CONSUMER_EXPENDITURES_TOOLS_AUTO",
        "HOUSEHOLD_CONSUMER_EXPENDITURES_GENERAL_MISC",
        "SRVY_HH_LIFESTYL_AFFILIATION_MEMBER_BOOK_CLUB",
        "SRVY_HH_LIFESTYL_CREDIT_CARDS_AMERICAN_EXPRESS_PR",
        "SRVY_HH_LIFESTYL_CREDIT_CARDS_AMERICAN_EXPRESS_RE",
        "SRVY_HH_LIFESTYL_CREDIT_CARDS_DISCOVER_PREMIUM",
        "SRVY_HH_LIFESTYL_CREDIT_CARDS_DISCOVER_REGULAR",
        "SRVY_HH_LIFESTYL_CREDIT_CARDS_OTHER_CARD_PREMIUM",
        "SRVY_HH_LIFESTYL_CREDIT_CARDS_OTHER_CARD_REGULAR",
        "SRVY_HH_LIFESTYL_CREDIT_CARDS_STORE_OR_RETAIL_REG",
        "SRVY_HH_LIFESTYL_CREDIT_CARDS_VISA_REGULAR",
        "SRVY_HH_LIFESTYL_CREDIT_CARDS_MASTERCARD_REGULAR",
        "SRVY_HH_LIFESTYL_FINANCIAL_CDS_MONEY_MKT_CUR",
        "SRVY_HH_LIFESTYL_FINANCIAL_IRAS_CURRENTLY",
        "SRVY_HH_LIFESTYL_FINANCIAL_STKS_BOND_CUR",
        "SRVY_HH_LIFESTYL_MILITARY_GOV_VETERN",
        "SRVY_HH_LIFESTYL_BUYING_COMP_ELEC",
        "SRVY_HH_LIFESTYL_BUYING_HOME_AND_GARDEN",
        "SRVY_HH_LIFESTYL_BUYING_SPORTS_RELATED",
        "CONSUMERVIEW_PROFITABILITY_SCORE",
        "NEW_MOVER_INDICATOR_LAST_6_MONTHS",
        "NEW_HOMEOWNER_INDICATOR_6M",
        "WORKING_COUPLES_DUAL_INCOME",
        "ACT_INT_AMUSEMENT_PARK_VISITORS",
        "ACT_INT_ZOO_VISITORS",
        "ACT_INT_WINE_LOVERS",
        "ACT_INT_DO_IT_YOURSELFERS",
        "ACT_INT_HOME_IMPROVEMENT_SPENDERS",
        "ACT_INT_HUNTING_ENTHUSIASTS",
        "BUYER_LAPTOP_OWNERS",
        "BUYER_SECURITY_SYSTEM_OWNERS",
        "BUYER_TABLET_OWNERS",
        "BUYER_COUPON_USERS",
        "BUYER_LUXURY_STORE_SHOPPERS",
        "BUYER_YOUNG_ADULT_CLOTHING_SHOPPERS",
        "BUYER_SUPERCENTER_SHOPPERS",
        "BUYER_WAREHOUSE_CLUB_MEMBERS",
        "MEMBERSHIPS_AARP_MEMBERS",
        "LIFESTYLE_LIFE_INSURANCE_POLICY_HOLDERS",
        "LIFESTYLE_MEDICAL_INSURANCE_POLICY_HOLDERS",
        "LIFESTYLE_MEDICARE_POLICY_HOLDERS",
        "ACT_INT_DIGITAL_MAGAZINE_NEWSPAPERS_BUYERS",
        "ACT_INT_ATTEND_ORDER_EDUCATIONAL_PROGRAMS",
        "ACT_INT_VIDEO_GAMER",
        "ACT_INT_MLB_ENTHUSIAST",
        "ACT_INT_NASCAR_ENTHUSIAST",
        "ACT_INT_NBA_ENTHUSIAST",
        "ACT_INT_NFL_ENTHUSIAST",
        "ACT_INT_NHL_ENTHUSIAST",
        "ACT_INT_PGA_TOUR_ENTHUSIAST",
        "ACT_INT_POLITICAL_VIEWING_ON_TV_LIBERAL",
        "ACT_INT_POLITICAL_VIEWING_ON_TV_LIBERAL_COMEDY",
        "ACT_INT_POLITICAL_VIEWING_ON_TV_CONSERVATIVE",
        "ACT_INT_EATS_AT_FAMILY_RESTAURANTS",
        "ACT_INT_EATS_AT_FAST_FOOD_RESTAURANTS",
        "ACT_INT_CANOEING_KAYAKING",
        "ACT_INT_PLAY_GOLF",
        "BUYER_LOYALTY_CARD_USER",
        "BUYER_LUXURY_HOME_GOODS_STORE_SHOPPER",
        "INVEST_HAVE_A_RETIREMENT_FINANCIAL_PLAN",
        "INVEST_PARTICIPATE_IN_ONLINE_TRADING",
        "LIFESTYLE_HAVE_GRANDCHILDREN",
        "BUYER_NON_PRESTIGE_MAKEUP_BRAND_USER",
        "ACT_INT_GOURMET_COOKING",
        "ACT_INT_CAT_OWNERS",
        "ACT_INT_DOG_OWNERS",
        "ACT_INT_ARTS_AND_CRAFTS",
        "ACT_INT_CULTURAL_ARTS",
        "HOBBIES_GARDENING",
        "ACT_INT_PHOTOGRAPHY",
        "ACT_INT_BOOK_READER",
        "ACT_INT_E_BOOK_READER",
        "ACT_INT_MUSIC_DOWNLOAD",
        "ACT_INT_MUSIC_STREAMING",
        "ACT_INT_AVID_RUNNERS",
        "ACT_INT_OUTDOOR_ENTHUSIAST",
        "ACT_INT_FISHING",
        "ACT_INT_SNOW_SPORTS",
        "ACT_INT_BOATING",
        "ACT_INT_PLAYS_SOCCER",
        "ACT_INT_PLAYS_TENNIS",
        "ACT_INT_SPORTS_ENTHUSIAST",
        "ACT_INT_HEALTHY_LIVING",
        "ACT_INT_FITNESS_ENTHUSIAST",
        "ACT_INT_WEIGHT_CONSCIOUS",
        "BUYER_HIGH_END_SPIRIT_DRINKERS",
        "ACT_INT_CASINO_GAMBLING",
        "LIFESTYLE_HIGH_FREQUENCY_CRUISE_ENTHUSIAST",
        "LIFESTYLE_HIGH_FREQUENCY_DOMESTIC_VACATIONER",
        "LIFESTYLE_HIGH_FREQUENCY_FOREIGN_VACATIONER",
        "LIFESTYLE_FREQUENT_FLYER_PROGRAM_MEMBER",
        "LIFESTYLE_HOTEL_GUEST_LOYALTY_PROGRAM",
        "DONOR_CONTRIBUTES_TO_CHARITIES",
        "DONOR_CONTRIBUTES_TO_ARTS_CULTURE_CHARITIES",
        "DONOR_CONTRIBUTES_TO_EDUCATION_CHARITIES",
        "DONOR_CONTRIBUTES_TO_HEALTH_CHARITIES",
        "DONOR_CONTRIBUTES_TO_POLITICAL_CHARITIES",
        "DONOR_CONTRIBUTES_TO_PRIVATE_FOUNDATIONS",
        "DONOR_CONTRIBUTES_BY_VOLUNTEERING",
        "FINANCIAL_DEBIT_CARD_USER",
        "FINANCIAL_MAJOR_CREDIT_CARD_USER",
        "FINANCIAL_STORE_CREDIT_CARD_USER",
        "INVEST_BROKERAGE_ACCOUNT_OWNER",
        "INVEST_ACTIVE_INVESTOR",
        "BUYER_PRESTIGE_MAKEUP_USER",
        "NEW_MOVER_DATE_LAST_6_MONTHS",
        "NEW_HOMEOWNER_DATE_LAST_6_MOS",
        "ACT_INT_LISTENS_TO_CHRISTIAN_MUSIC",
        "ACT_INT_LISTENS_TO_CLASSICAL_MUSIC",
        "TRUETOUCH_ENG_MOBILE_SMS_MMS",
        "TRUETOUCH_ENG_TRADITIONAL_NEWSPAPER",
        "ACT_INT_LISTENS_TO_COUNTRY_MUSIC",
        "ACT_INT_LISTENS_TO_MUSIC",
        "TRUETOUCH_ENG_STREAMING_TV",
        "TRUETOUCH_ENG_RADIO",
        "ACT_INT_LISTENS_TO_OLDIES_MUSIC",
        "ACT_INT_LISTENS_TO_ROCK_MUSIC",
        "TRUETOUCH_ENG_DIGITAL_VIDEO",
        "TRUETOUCH_ENG_DIGITAL_NEWSPAPER",
        "ACT_INT_LISTENS_TO_80S_MUSIC",
        "ACT_INT_LISTENS_TO_HIP_HOP_MUSIC",
        "TRUETOUCH_ENG_DIRECT_MAIL",
        "TRUETOUCH_ENG_DIGITAL_DISPLAY",
        "ACT_INT_LISTENS_TO_ALTERNATIVE_MUSIC",
        "ACT_INT_LISTENS_TO_JAZZ_MUSIC",
        "TRUETOUCH_ENG_BROADCAST_CABLE_TV",
        "TRUETOUCH_NOVELTY_SEEKERS",
        "ACT_INT_LISTENS_TO_POP_MUSIC",
        "LIFESTYLE_INTEREST_IN_RELIGION",
        "TRUETOUCH_MAINSTREAM_ADOPTERS",
        "TRUETOUCH_IN_THE_MOMENT_SHOPPERS",
        "PERSON_NUM_EDUCATION_MODEL1",
        "PERSON_NUM_MARITAL_STATUS1",
        "TRUETOUCH_QUALITY_MATTERS",
        "TRUETOUCH_RECREATIONAL_SHOPPERS",
        "PERSON_NUM_MAIL_RESPONDER1",
        "TRUETOUCH_DEAL_SEEKERS",
        "PERSON_NUM_POLITICALPERSONA_I1",
        "PERSON_NUM_BIRTH_YEAR_AND_MONTH1",
        "TRUETOUCH_TRENDSETTERS",
        "TRUETOUCH_BRAND_LOYALISTS",
        "PERSON_NUM_BUSINESS_OWNER1",
        "PERSON_NUM_COMBINED_AGE1",
        "TRUETOUCH_ORGANIC_AND_NATURAL",
        "TRUE_TOUCH_SAVVY_RESEARCHERS",
        "PERSON_NUM_GENDER1",
        "TT_CONV_SPECIALTY_OR_BOUTIQUE",
        "TT_CONV_WHOLESALE",
        "PERSON_NUM_OCCUPATION_CODE1",
        "PERSON_NUM_PERSON_TYPE1",
        "TT_CONV_SPECIALTY_DEPT_STORE",
        "TT_CONV_MID_HIGH_END_STORE",
        "TT_CONV_ETAIL_ONLY",
        "ESTIMATED_HOUSEHOLD_INCOME_AMOUNT_V6",
        "PERSON_NUM_POLITICAL_AFFILIATION1",
        "ESTIMATED_HOUSEHOLD_INCOME_RANGE_CODE_V6",
        "TT_CONV_EBID_SITES",
        "TT_CONV_DISCOUNT_SUPERCENTERS",
        "PERSON_NUM_OCCUPATION_GROUP_V21",
        "TT_CONV_ONLINE_DEAL_VOUCHER",
        "PERSON_NUM_HEALTH_AND_WELL_BEING1",
        "PERSON_NUM_MOBILE_USERS1",
        CASE WHEN PII_ACCESS = TRUE THEN "TRUETOUCH_ENG_EMAIL" ELSE sha2("TRUETOUCH_ENG_EMAIL", 512) END AS "TRUETOUCH_ENG_EMAIL",
        "PERSON_NUM_TECHNOLOGY_ADOPTION1",
        "PERSON_NUM_FICA_FLAG1",
        "PERSON_NUM_EDUCATION_MODEL2",
        "PERSON_NUM_MARITAL_STATUS2",
        "PERSON_NUM_MAIL_RESPONDER2",
        "PERSON_NUM_POLITICALPERSONA_I1_2",
        "PERSON_NUM_BIRTH_YEAR_AND_MONTH2",
        "PERSON_NUM_BUSINESS_OWNER2",
        "PERSON_NUM_COMBINED_AGE2",
        "PERSON_NUM_GENDER2",
        "PERSON_NUM_OCCUPATION_CODE2",
        "PERSON_NUM_PERSON_TYPE2",
        "PERSON_NUM_POLITICAL_AFFILIATION2",
        "PERSON_NUM_OCCUPATION_GROUP_V22",
        "PERSON_NUM_HEALTH_AND_WELL_BEING2",
        "PERSON_NUM_MOBILE_USERS2",
        "PERSON_NUM_TECHNOLOGY_ADOPTION2",
        "PERSON_NUM_FICA_FLAG2",
        "PERSON_NUM_EDUCATION_MODEL3",
        "PERSON_NUM_MARITAL_STATUS3",
        "PERSON_NUM_MAIL_RESPONDER3",
        "PERSON_NUM_POLITICALPERSONA_I1_3",
        "PERSON_NUM_BIRTH_YEAR_AND_MONTH3",
        "PERSON_NUM_BUSINESS_OWNER3",
        "PERSON_NUM_COMBINED_AGE3",
        "PERSON_NUM_GENDER3",
        "PERSON_NUM_OCCUPATION_CODE3",
        "PERSON_NUM_PERSON_TYPE3",
        "PERSON_NUM_POLITICAL_AFFILIATION3",
        "PERSON_NUM_OCCUPATION_GROUP_V23",
        "PERSON_NUM_HEALTH_AND_WELL_BEING3",
        "PERSON_NUM_MOBILE_USERS3",
        "PERSON_NUM_TECHNOLOGY_ADOPTION3",
        "PERSON_NUM_FICA_FLAG3",
        "PERSON_NUM_EDUCATION_MODEL4",
        "PERSON_NUM_MARITAL_STATUS4",
        "PERSON_NUM_MAIL_RESPONDER4",
        "PERSON_NUM_POLITICALPERSONA_I1_4",
        "PERSON_NUM_BIRTH_YEAR_AND_MONTH4",
        "PERSON_NUM_BUSINESS_OWNER4",
        "PERSON_NUM_COMBINED_AGE4",
        "PERSON_NUM_GENDER4",
        "PERSON_NUM_OCCUPATION_CODE4",
        "PERSON_NUM_PERSON_TYPE4",
        "PERSON_NUM_POLITICAL_AFFILIATION4",
        "PERSON_NUM_OCCUPATION_GROUP_V24",
        "PERSON_NUM_HEALTH_AND_WELL_BEING4",
        "PERSON_NUM_MOBILE_USERS4",
        "PERSON_NUM_TECHNOLOGY_ADOPTION4",
        "PERSON_NUM_FICA_FLAG4",
        "PERSON_NUM_EDUCATION_MODEL5",
        "PERSON_NUM_MARITAL_STATUS5",
        "PERSON_NUM_MAIL_RESPONDER5",
        "PERSON_NUM_POLITICALPERSONA_I1_5",
        "PERSON_NUM_BIRTH_YEAR_AND_MONTH5",
        "PERSON_NUM_BUSINESS_OWNER5",
        "PERSON_NUM_COMBINED_AGE5",
        "PERSON_NUM_GENDER5",
        "PERSON_NUM_OCCUPATION_CODE5",
        "PERSON_NUM_PERSON_TYPE5",
        "PERSON_NUM_POLITICAL_AFFILIATION5",
        "PERSON_NUM_OCCUPATION_GROUP_V25",
        "PERSON_NUM_HEALTH_AND_WELL_BEING5",
        "PERSON_NUM_MOBILE_USERS5",
        "PERSON_NUM_TECHNOLOGY_ADOPTION5",
        "PERSON_NUM_FICA_FLAG5",
        "PERSON_NUM_EDUCATION_MODEL6",
        "PERSON_NUM_MARITAL_STATUS6",
        "PERSON_NUM_MAIL_RESPONDER6",
        "PERSON_NUM_POLITICALPERSONA_I1_6",
        "PERSON_NUM_BIRTH_YEAR_AND_MONTH6",
        "PERSON_NUM_BUSINESS_OWNER6",
        "PERSON_NUM_COMBINED_AGE6",
        "PERSON_NUM_GENDER6",
        "PERSON_NUM_OCCUPATION_CODE6",
        "PERSON_NUM_PERSON_TYPE6",
        "PERSON_NUM_POLITICAL_AFFILIATION6",
        "PERSON_NUM_OCCUPATION_GROUP_V26",
        "PERSON_NUM_HEALTH_AND_WELL_BEING6",
        "PERSON_NUM_MOBILE_USERS6",
        "PERSON_NUM_TECHNOLOGY_ADOPTION6",
        "PERSON_NUM_FICA_FLAG6",
        "PERSON_NUM_EDUCATION_MODEL7",
        "PERSON_NUM_MARITAL_STATUS7",
        "PERSON_NUM_MAIL_RESPONDER7",
        "PERSON_NUM_POLITICALPERSONA_I1_7",
        "PERSON_NUM_BIRTH_YEAR_AND_MONTH7",
        "PERSON_NUM_BUSINESS_OWNER7",
        "PERSON_NUM_COMBINED_AGE7",
        "PERSON_NUM_GENDER7",
        "PERSON_NUM_OCCUPATION_CODE7",
        "PERSON_NUM_PERSON_TYPE7",
        "PERSON_NUM_POLITICAL_AFFILIATION7",
        "PERSON_NUM_OCCUPATION_GROUP_V27",
        "PERSON_NUM_HEALTH_AND_WELL_BEING7",
        "PERSON_NUM_MOBILE_USERS7",
        "PERSON_NUM_TECHNOLOGY_ADOPTION7",
        "PERSON_NUM_FICA_FLAG7",
        "PERSON_NUM_EDUCATION_MODEL8",
        "PERSON_NUM_MARITAL_STATUS8",
        "PERSON_NUM_MAIL_RESPONDER8",
        "PERSON_NUM_POLITICALPERSONA_I1_8",
        "PERSON_NUM_BIRTH_YEAR_AND_MONTH8",
        "PERSON_NUM_BUSINESS_OWNER8",
        "PERSON_NUM_COMBINED_AGE8",
        "PERSON_NUM_GENDER8",
        "PERSON_NUM_OCCUPATION_CODE8",
        "PERSON_NUM_PERSON_TYPE8",
        "PERSON_NUM_POLITICAL_AFFILIATION8",
        "PERSON_NUM_OCCUPATION_GROUP_V28",
        "PERSON_NUM_HEALTH_AND_WELL_BEING8",
        "PERSON_NUM_MOBILE_USERS8",
        "PERSON_NUM_TECHNOLOGY_ADOPTION8",
        "PERSON_NUM_FICA_FLAG8",
        "PERSON_NUM_NAME_PREFIX",
        CASE WHEN PII_ACCESS = TRUE THEN "PERSON_NUM_FIRST_NAME" ELSE sha2("PERSON_NUM_FIRST_NAME", 512) END AS "PERSON_NUM_FIRST_NAME",
        CASE WHEN PII_ACCESS = TRUE THEN "PERSON_NUM_MIDDLE_INITIAL_V2" ELSE sha2("PERSON_NUM_MIDDLE_INITIAL_V2", 512) END AS "PERSON_NUM_MIDDLE_INITIAL_V2",
        CASE WHEN PII_ACCESS = TRUE THEN "PERSON_NUM_SURNAME" ELSE sha2("PERSON_NUM_SURNAME", 512) END AS "PERSON_NUM_SURNAME",
        CASE WHEN PII_ACCESS = TRUE THEN "PERSON_NUM_NAME_SUFFIX" ELSE sha2("PERSON_NUM_NAME_SUFFIX", 512) END AS "PERSON_NUM_NAME_SUFFIX",
        CASE WHEN PII_ACCESS = TRUE THEN "PRIMARY_ADDRESS" ELSE sha2("PRIMARY_ADDRESS", 512) END AS "PRIMARY_ADDRESS",
        CASE WHEN PII_ACCESS = TRUE THEN "SECONDARY_ADDRESS" ELSE sha2("SECONDARY_ADDRESS", 512) END AS "SECONDARY_ADDRESS",
        "CITY_NAME_ABBREVIATED",
        "STATE_ABBREVIATION",
        "ZIP_CODE"
 FROM "DATAENGINEERING"."MARKETING"."EXPERIAN_PIPE_DELIM_PII"
 LEFT JOIN "DATAENG_UTILS"."MAPPINGS"."PII_MAPPINGS" PII ON PII.ROLE = CURRENT_ROLE()
);