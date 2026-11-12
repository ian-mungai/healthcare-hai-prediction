function initialize_menu_values(callback) {
    cond_domains=[
    {
        'disp':get_string_by_id("39"),
        'val':"d1"
    },
    {
        'disp':get_string_by_id("40"),
        'val':"d2"
    },
    {
        'disp': get_string_by_id("331"),
        'val': "d5"
    }
    ];
        //Behavioral health conditions for 2021 and later
        BH_conditions=[
            {'disp': get_string_by_id("161"), 'val': '42', 'header': false, 'domain': 5, 'measure': 'vatphe'},
            {'disp': get_string_by_id("309"), 'val': '136', 'header': false, 'domain': 5, 'measure': 'vatphe'},
            {'disp': get_string_by_id("238"), 'val': '1', 'header': false, 'domain': 5, 'measure': 'vatphe'},
            {'disp': get_string_by_id("162"), 'val': '43', 'header': false, 'domain': 5, 'measure': 'vatphe'},
            {'disp': get_string_by_id("164"), 'val': '45', 'header': false, 'domain': 5, 'measure': 'vatphe'},
            {'disp': get_string_by_id("50"), 'val': '14', 'header': false, 'domain': 5, 'measure': 'vatphe'},
            {'disp': get_string_by_id("168"), 'val': '49', 'header': false, 'domain': 5, 'measure': 'vatphe'},
            {'disp': get_string_by_id("310"), 'val': '137', 'header': false, 'domain': 5, 'measure': 'vatphe'},
            {'disp': get_string_by_id("187"), 'val': '64', 'header': false, 'domain': 5, 'measure': 'vatphe'},
            {'disp': get_string_by_id("188"), 'val': '65', 'header': false, 'domain': 5, 'measure': 'vatphe'},
            {'disp': get_string_by_id("190"), 'val': '41', 'header': false, 'domain': 5, 'measure': 'vatphe'},
            {'disp': get_string_by_id("60"), 'val': '22', 'header': false, 'domain': 5, 'measure': 'vatphe'},
            {'disp': get_string_by_id("195"), 'val': '69', 'header': false, 'domain': 5, 'measure': 'vatphe'},
            {'disp': get_string_by_id("261"), 'val': '109', 'header': false, 'domain': 5, 'measure': 'vathe'},
            {'disp': get_string_by_id("262"), 'val': '110', 'header': false, 'domain': 5, 'measure': 'v'},
            {'disp': get_string_by_id("263"), 'val': '111', 'header': false, 'domain': 5, 'measure': 'v'},
            {'disp': get_string_by_id("264"), 'val': '112', 'header': false, 'domain': 5, 'measure': 'v'},
        ];

        //Behavioral health conditions before 2021
        BH_conditions_pre2021=[
            {'disp': get_string_by_id("161"), 'val': '42', 'header': false, 'domain': 5, 'measure': 'vatphe'},
            {'disp': get_string_by_id("238"), 'val': '1', 'header': false, 'domain': 5, 'measure': 'vatphe'},
            {'disp': get_string_by_id("162"), 'val': '43', 'header': false, 'domain': 5, 'measure': 'vatphe'},
            {'disp': get_string_by_id("164"), 'val': '45', 'header': false, 'domain': 5, 'measure': 'vatphe'},
            {'disp': get_string_by_id("50"), 'val': '14', 'header': false, 'domain': 5, 'measure': 'vatphe'},
            {'disp': get_string_by_id("168"), 'val': '49', 'header': false, 'domain': 5, 'measure': 'vatphe'},
            {'disp': get_string_by_id("187"), 'val': '64', 'header': false, 'domain': 5, 'measure': 'vatphe'},
            {'disp': get_string_by_id("188"), 'val': '65', 'header': false, 'domain': 5, 'measure': 'vatphe'},
            {'disp': get_string_by_id("190"), 'val': '41', 'header': false, 'domain': 5, 'measure': 'vatphe'},
            {'disp': get_string_by_id("60"), 'val': '22', 'header': false, 'domain': 5, 'measure': 'vatphe'},
            {'disp': get_string_by_id("195"), 'val': '69', 'header': false, 'domain': 5, 'measure': 'vatphe'},
            {'disp': get_string_by_id("261"), 'val': '109', 'header': false, 'domain': 5, 'measure': 'vathe'},
            {'disp': get_string_by_id("262"), 'val': '110', 'header': false, 'domain': 5, 'measure': 'v'},
            {'disp': get_string_by_id("263"), 'val': '111', 'header': false, 'domain': 5, 'measure': 'v'},
            {'disp': get_string_by_id("264"), 'val': '112', 'header': false, 'domain': 5, 'measure': 'v'}
        ];
    
    adjust_cond_domains=[
        {
            'disp':get_string_by_id("39"),
            'val':"d1"
        },
        {
            'disp':get_string_by_id("40"),
            'val':"d2"
        },
        {
            'disp': get_string_by_id("331"),
            'val': "d5"
        },
        {
            'disp':"COVID-19",
            'val':"d4"
        }
        ];


    preventive_domains=[
        {
            'disp': get_string_by_id("24"),
            'val': null
        }
    ];

    MA_domains=[{
        'disp': get_string_by_id("330"),
            'val': null
    }];

    //Conditions for MA data
    MA_conditions_discharge=[
        {'disp': get_string_by_id("275"), 'val': '130', 'header': false, 'domain': null, 'measure': 'd'},
        {'disp': get_string_by_id("276"), 'val': '131', 'header': false, 'domain': null, 'measure': 'd'},
        {'disp': get_string_by_id("277"), 'val': '132', 'header': false, 'domain': null, 'measure': 'd'},
        {'disp': get_string_by_id("278"), 'val': '133', 'header': false, 'domain': null, 'measure': 'd'}
    ];
    MA_conditions=[
        {'disp': get_string_by_id("240"), 'val': '10', 'header': false, 'domain': 1, 'measure': 'h'},
    ];

    other_domains=[
    {
        'disp':get_string_by_id("39"),
        'val': null
    }
    ];

    conditions = [
    {'disp': get_string_by_id("239"), 'val': '10', 'header': false, 'domain': 1, 'measure': 'e'},
    {'disp': get_string_by_id("240"), 'val': '10', 'header': false, 'domain': 1, 'measure': 'h'},
    {'disp': get_string_by_id("241"), 'val': '10', 'header': false, 'domain': 1, 'measure': 'r'},
    {'disp': "----", 'val': '', 'header': false, 'domain': 1, 'measure': 're', 'dib': true},
    {'disp': get_string_by_id("42"), 'val': '2', 'header': false, 'domain': 1, 'measure': 'vatphmr'},
    {'disp': get_string_by_id("238"), 'val': '1', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("244"), 'val': '26', 'header': false, 'domain': 0, 'measure': 'q'},
    {'disp': get_string_by_id("343"), 'val': '147', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("44"), 'val': '4', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("46"), 'val': '11', 'header': false, 'domain': 1, 'measure': 'vatpe'},
    {'disp': get_string_by_id("245"), 'val': '27', 'header': false, 'domain': 0, 'measure': 'q'},
    {'disp': get_string_by_id("344"), 'val': '148', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("47x"), 'val': '5', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("47x2"), 'val': '78', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("47x1"), 'val': '79', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("345"), 'val': '149', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("47x4"), 'val': '80', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("47x3"), 'val': '81', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("346"), 'val': '150', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("347"), 'val': '151', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("258"), 'val': '11', 'header': false, 'domain': 1, 'measure': 'h'},
    {'disp': get_string_by_id("48"), 'val': '12', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("49"), 'val': '13', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("246"), 'val': '28', 'header': false, 'domain': 0, 'measure': 'q'},
    {'disp': get_string_by_id("247"), 'val': '29', 'header': false, 'domain': 0, 'measure': 'q'},
    {'disp': get_string_by_id("50"), 'val': '14', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("50"), 'val': '14', 'header': false, 'domain': 5, 'measure': 'vatphe'},
    {'disp': get_string_by_id("51"), 'val': '15', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("248"), 'val': '30', 'header': false, 'domain': 0, 'measure': 'q'},
    {'disp': get_string_by_id("249"), 'val': '31', 'header': false, 'domain': 0, 'measure': 'q'},
    {'disp': get_string_by_id("52"), 'val': '24', 'header': false, 'domain': 1, 'measure': 'vathe'},
    {'disp': get_string_by_id("53"), 'val': '25', 'header': false, 'domain': 1, 'measure': 'vathe'},
    {'disp': get_string_by_id("349"), 'val': '153', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("54"), 'val': '16', 'header': false, 'domain': 1, 'measure': 'vatphmre'},
    {'disp': get_string_by_id("348"), 'val': '152', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("250"), 'val': '32', 'header': false, 'domain': 0, 'measure': 'q'},
    {'disp': get_string_by_id("55"), 'val': '18', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("56"), 'val': '17', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("350"), 'val': '154', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("251"), 'val': '33', 'header': false, 'domain': 0, 'measure': 'q'},
    {'disp': get_string_by_id("57"), 'val': '19', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("252"), 'val': '34', 'header': false, 'domain': 0, 'measure': 'q'},
    {'disp': get_string_by_id("58"), 'val': '20', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("59"), 'val': '21', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("351"), 'val': '155', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("352"), 'val': '156', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("329"), 'val': '35', 'header': false, 'domain': 0, 'measure': 'q'},
    {'disp': get_string_by_id("253"), 'val': '36', 'header': false, 'domain': 0, 'measure': 'q'},
    {'disp': get_string_by_id("254"), 'val': '37', 'header': false, 'domain': 0, 'measure': 'q'},
    {'disp': get_string_by_id("255"), 'val': '38', 'header': false, 'domain': 0, 'measure': 'q'},
    {'disp': get_string_by_id("45"), 'val': '3', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("60"), 'val': '22', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("60"), 'val': '22', 'header': false, 'domain': 5, 'measure': 'vatphe'},
    {'disp': get_string_by_id("61"), 'val': '23', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("256"), 'val': '39', 'header': false, 'domain': 0, 'measure': 'q'},
    {'disp': get_string_by_id("257"), 'val': '40', 'header': false, 'domain': 0, 'measure': 'q'},
    {'disp': "----", 'val': '', 'header': false, 'domain': 1, 'measure': 'vatphe', 'dib': true},
    {'disp': get_string_by_id("62"), 'val': '6', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("63"), 'val': '7', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("64"), 'val': '8', 'header': false, 'domain': 1, 'measure': 'vatphe'},
    {'disp': get_string_by_id("65"), 'val': '9', 'header': false, 'domain': 1, 'measure': 'vatphe'},

    {'disp': get_string_by_id("166"), 'val': '72', 'header': true, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("161"), 'val': '42', 'header': false, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("163"), 'val': '44', 'header': false, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("165"), 'val': '47', 'header': false, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("167"), 'val': '48', 'header': false, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("172"), 'val': '55', 'header': false, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("173"), 'val': '56', 'header': false, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("175"), 'val': '73', 'header': true, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("176"), 'val': '58', 'header': false, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("197"), 'val': '53', 'header': false, 'domain': 2, 'measure': 'vatphe'},
    //BH conditions
    {'disp': get_string_by_id("161"), 'val': '42', 'header': false, 'domain': 5, 'measure': 'vatphe'},
    {'disp': get_string_by_id("238"), 'val': '1', 'header': false, 'domain': 5, 'measure': 'vatphe'},
    {'disp': get_string_by_id("162"), 'val': '43', 'header': false, 'domain': 5, 'measure': 'vatphe'},
    {'disp': get_string_by_id("164"), 'val': '45', 'header': false, 'domain': 5, 'measure': 'vatphe'},
    {'disp': get_string_by_id("168"), 'val': '49', 'header': false, 'domain': 5, 'measure': 'vatphe'},
    {'disp': get_string_by_id("187"), 'val': '64', 'header': false, 'domain': 5, 'measure': 'vatphe'},
    {'disp': get_string_by_id("188"), 'val': '65', 'header': false, 'domain': 5, 'measure': 'vatphe'},
    {'disp': get_string_by_id("190"), 'val': '41', 'header': false, 'domain': 5, 'measure': 'vatphe'},
    {'disp': get_string_by_id("195"), 'val': '69', 'header': false, 'domain': 5, 'measure': 'vatphe'},
    {'disp': get_string_by_id("261"), 'val': '109', 'header': false, 'domain': 5, 'measure': 'vathe'},
    {'disp': get_string_by_id("262"), 'val': '110', 'header': false, 'domain': 5, 'measure': 'v'},
    {'disp': get_string_by_id("263"), 'val': '111', 'header': false, 'domain': 5, 'measure': 'v'},
    {'disp': get_string_by_id("264"), 'val': '112', 'header': false, 'domain': 5, 'measure': 'v'},
    {'disp': get_string_by_id("180"), 'val': '75', 'header': true, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("170"), 'val': '51', 'header': false, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("179"), 'val': '60', 'header': false, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("181"), 'val': '61', 'header': false, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("182"), 'val': '62', 'header': false, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("183"), 'val': '76', 'header': true, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("169"), 'val': '50', 'header': false, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("178"), 'val': '59', 'header': false, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("193"), 'val': '67', 'header': false, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("194"), 'val': '68', 'header': false, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("196"), 'val': '46', 'header': false, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("184"), 'val': '77', 'header': true, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("171"), 'val': '54', 'header': false, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("174"), 'val': '57', 'header': false, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("186"), 'val': '66', 'header': false, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("189"), 'val': '70', 'header': false, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("191"), 'val': '71', 'header': false, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("192"), 'val': '52', 'header': false, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("341"), 'val': '144', 'header': false, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("185"), 'val': '63', 'header': true, 'domain': 2, 'measure': 'vatphe'},
    {'disp': get_string_by_id("199"), 'val': '82', 'header': false, 'domain': 3, 'measure': 'c'},
    {'disp': get_string_by_id("200"), 'val': '83', 'header': false, 'domain': 3, 'measure': 'c'},
    {'disp': get_string_by_id("201"), 'val': '84', 'header': false, 'domain': 3, 'measure': 'c'},
    {'disp': get_string_by_id("202"), 'val': '85', 'header': false, 'domain': 3, 'measure': 'c'},
    {'disp': get_string_by_id("203"), 'val': '86', 'header': false, 'domain': 3, 'measure': 'c'},
    {'disp': get_string_by_id("204"), 'val': '87', 'header': false, 'domain': 3, 'measure': 'c'},
    {'disp': get_string_by_id("205"), 'val': '88', 'header': false, 'domain': 3, 'measure': 'c'},
    {'disp': get_string_by_id("206"), 'val': '89', 'header': false, 'domain': 3, 'measure': 'c'},
    {'disp': get_string_by_id("207"), 'val': '90', 'header': false, 'domain': 3, 'measure': 'c'},
    {'disp': get_string_by_id("208"), 'val': '91', 'header': false, 'domain': 3, 'measure': 'c'},
    {'disp': get_string_by_id("209"), 'val': '92', 'header': false, 'domain': 3, 'measure': 'c'},
    {'disp': get_string_by_id("210"), 'val': '93', 'header': false, 'domain': 3, 'measure': 'c'},
    {'disp': get_string_by_id("211"), 'val': '94', 'header': false, 'domain': 3, 'measure': 'c'},
    {'disp': get_string_by_id("212"), 'val': '95', 'header': false, 'domain': 3, 'measure': 'c'},
    {'disp': get_string_by_id("213"), 'val': '96', 'header': false, 'domain': 3, 'measure': 'c'},
    {'disp': get_string_by_id("214"), 'val': '97', 'header': false, 'domain': 3, 'measure': 'c'},
    {'disp': get_string_by_id("215"), 'val': '98', 'header': false, 'domain': 3, 'measure': 'c'},
    {'disp': get_string_by_id("216"), 'val': '99', 'header': false, 'domain': 3, 'measure': 'c'},
    {'disp': get_string_by_id("217"), 'val': '100', 'header': false, 'domain': 3, 'measure': 'c'},
    {'disp': get_string_by_id("218"), 'val': '101', 'header': false, 'domain': 3, 'measure': 'c'},
    {'disp': get_string_by_id("219"), 'val': '102', 'header': false, 'domain': 3, 'measure': 'c'},
    {'disp': get_string_by_id("220"), 'val': '103', 'header': false, 'domain': 3, 'measure': 'c'},
    {'disp': get_string_by_id("221"), 'val': '104', 'header': false, 'domain': 3, 'measure': 'c'},
    {'disp': get_string_by_id("222"), 'val': '105', 'header': false, 'domain': 3, 'measure': 'c'},
    {'disp': get_string_by_id("223"), 'val': '106', 'header': false, 'domain': 3, 'measure': 'c'},
    {'disp': get_string_by_id("224"), 'val': '107', 'header': false, 'domain': 3, 'measure': 'c'},
    {'disp': get_string_by_id("225"), 'val': '108', 'header': false, 'domain': 3, 'measure': 'c'},
    //{'disp': get_string_by_id("261"), 'val': '109', 'header': false, 'domain': 1, 'measure': 'e'},
    {'disp': get_string_by_id("279"), 'val': '113', 'header': false, 'domain': 3, 'measure': 's'},
    {'disp': get_string_by_id("280"), 'val': '114', 'header': false, 'domain': 3, 'measure': 's'},
    {'disp': get_string_by_id("281"), 'val': '115', 'header': false, 'domain': 3, 'measure': 's'},
    {'disp': get_string_by_id("282"), 'val': '116', 'header': false, 'domain': 3, 'measure': 's'},
    {'disp': get_string_by_id("283"), 'val': '117', 'header': false, 'domain': 3, 'measure': 's'},
    {'disp': get_string_by_id("284"), 'val': '118', 'header': false, 'domain': 3, 'measure': 's'},
    {'disp': get_string_by_id("285"), 'val': '119', 'header': false, 'domain': 3, 'measure': 's'},
    {'disp': get_string_by_id("286"), 'val': '120', 'header': false, 'domain': 3, 'measure': 's'},
    {'disp': get_string_by_id("287"), 'val': '121', 'header': false, 'domain': 3, 'measure': 's'},
    {'disp': get_string_by_id("288"), 'val': '122', 'header': false, 'domain': 3, 'measure': 's'},
    {'disp': get_string_by_id("289"), 'val': '123', 'header': false, 'domain': 3, 'measure': 's'},
    {'disp': get_string_by_id("290"), 'val': '124', 'header': false, 'domain': 3, 'measure': 's'},
    {'disp': get_string_by_id("291"), 'val': '125', 'header': false, 'domain': 3, 'measure': 's'},
    {'disp': get_string_by_id("271"), 'val': '126', 'header': false, 'domain': 3, 'measure': 'n'},
    {'disp': get_string_by_id("272"), 'val': '127', 'header': false, 'domain': 3, 'measure': 'n'},
    {'disp': get_string_by_id("273"), 'val': '128', 'header': false, 'domain': 3, 'measure': 'n'},
    {'disp': get_string_by_id("274"), 'val': '129', 'header': false, 'domain': 3, 'measure': 'n'},
    {'disp': get_string_by_id("275"), 'val': '130', 'header': false, 'domain': 3, 'measure': 'd'},
    {'disp': get_string_by_id("276"), 'val': '131', 'header': false, 'domain': 3, 'measure': 'd'},
    {'disp': get_string_by_id("277"), 'val': '132', 'header': false, 'domain': 3, 'measure': 'd'},
    {'disp': get_string_by_id("278"), 'val': '133', 'header': false, 'domain': 3, 'measure': 'd'},
    {'disp': get_string_by_id("292"), 'val': '', 'header': false, 'domain': 3, 'measure': 'i'},
    {'disp': get_string_by_id("293"), 'val': '', 'header': false, 'domain': 3, 'measure': 'b'},
    {'disp': get_string_by_id("312"), 'val': '139', 'header': false, 'domain': 0, 'measure': 'ou'},
    {'disp': get_string_by_id("311"), 'val': '138', 'header': false, 'domain': 0, 'measure': 'ou'},
    {'disp': get_string_by_id("313"), 'val': '140', 'header': false, 'domain': 0, 'measure': 'ou'},
    {'disp': get_string_by_id("314"), 'val': '141', 'header': false, 'domain': 0, 'measure': 'ou'},
    {'disp': get_string_by_id("315"), 'val': '142', 'header': false, 'domain': 0, 'measure': 'ou'},
    {'disp': get_string_by_id("316"), 'val': '143', 'header': false, 'domain': 0, 'measure': 'ou'},
    {'disp': get_string_by_id("330"), 'val': '10', 'header': false, 'domain': 0, 'measure': '1234567'} 
    ];


    state_analyses = [{
            'disp': get_string_by_id("32"),
            'val': "base"
        },
        {
            'disp': get_string_by_id("242"),
            'val': "countydiff"
        },
        {
            'disp': get_string_by_id("34"),
            'val': "diffindiff",
            'dib': null
        },
        {
            'disp': get_string_by_id("35"),
            'val': "statediff",
            'dib': null
        },
        {
          'disp': get_string_by_id("265"),
            'val': "ruralurbandiff"
        },
        {
            'disp': get_string_by_id("36"),
            'val': "nationdiff"
        },
        {
            'disp': get_string_by_id("37"),
            'val': null,
            'dib': null
        }
    ];

    MA_analyses = [{
        'disp': get_string_by_id("32"),
        'val': "base"
    },
    {
        'disp': get_string_by_id("242"),
        'val': "countydiff",
       
    },
    {
        'disp': get_string_by_id("34"),
        'val': "diffindiff",
        'dib': null
    },
    {
        'disp': get_string_by_id("35"),
        'val': "statediff",
        'dib': null
        
    },
    {
      'disp': get_string_by_id("265"),
        'val': "ruralurbandiff",
        'dib': null
    },
    {
        'disp': get_string_by_id("36"),
        'val': "nationdiff"
    },
    {
        'disp': get_string_by_id("37"),
        'val': null,
        'dib': null
    }
];

    market_metrics_analyses = [{
        'disp': get_string_by_id("32"),
        'val': "base"
    },
    {
        'disp': get_string_by_id("33"),
        'val': "countydiff",
        'dib': null
    },
    {
        'disp': get_string_by_id("34"),
        'val': "diffindiff",
        'dib': null
    },
    {
        'disp': get_string_by_id("35"),
        'val': "statediff"
    },
    {
        'disp': get_string_by_id("36"),
        'val': "nationdiff",
        'dib': null
    },
    {
        'disp': get_string_by_id("37"),
        'val': null,
        'dib': null
    }
];

//Suppresses smoothed values
    adjust_unsmooth_suppress = [{
                    'disp': get_string_by_id("27"),
                    'val': "1"
                },
                {
                    'disp': get_string_by_id("28"),
                    'val': "2"
                }
            ]

            //Suppresses smoothed values
    adjustment_unsmoothed_only = [{
        'disp': get_string_by_id("27"),
        'val': "1"
    }
];

//Adds COVID-19 condition
adjust_conditions =  [{
    'disp': "COVID-19",
    'val': "134"
    //'header': false, 
    //'domain': 1, 
    //'measure': 'e'
}
];

//COVID-19 measures
measure_covid = [{
    'disp': get_string_by_id("295"),
    'header': true,
    'dib': true,
},
{
    'disp': get_string_by_id("20"),
    'sel': true,
    'val': "h"
},
{
    'disp': get_string_by_id("22"),
    'val': "v"
}
    ];

 //Suppresses inpatient measures
    measure_inpatient_suppress = [{
        'disp': get_string_by_id("295"),
        'header': true,
        'dib': true,
    },
    {
        'disp': get_string_by_id("16"),
        'sel': true,
        'val': "p"
    },
    {
        'disp': get_string_by_id("17"),
        'val': "t"
    },
    {
        'disp': get_string_by_id("18"),
        'val': "a"
    },
    {
        'disp': get_string_by_id("19"),
        'val': "e"
    },
    {
        'disp': get_string_by_id("20"),
        'val': "h"
    },
    {
        'disp': get_string_by_id("21"),
        'val': "m"
    },
    {
        'disp': get_string_by_id("22"),
        'val': "v"
    },
    {
        'disp': get_string_by_id("23"),
        'val': "q"
    },
    {
        'disp': get_string_by_id("24"),
        'val': "c"
    },
    {
        'disp': get_string_by_id("25"),
        'val': "r"
    }
        ];

    //Supresses the Number of Providers measure, which is only available from 2014 onward
    measure_pre2014 = [{
        'disp': get_string_by_id("295"),
        'header': true,
        'dib': true,
    },
    {
        'disp': get_string_by_id("16"),
        'sel': true,
        'val': "p"
    },
    {
        'disp': get_string_by_id("17"),
        'val': "t"
    },
    {
        'disp': get_string_by_id("18"),
        'val': "a"
    },
    {
        'disp': get_string_by_id("19"),
        'val': "e"
    },
    {
        'disp': get_string_by_id("20"),
        'val': "h"
    },
    {
        'disp': get_string_by_id("21"),
        'val': "m"
    },
    {
        'disp': get_string_by_id("22"),
        'val': "v"
    },
    {
        'disp': get_string_by_id("23"),
        'val': "q"
    },
    {
        'disp': get_string_by_id("24"),
        'val': "c"
    },
    {
        'disp': get_string_by_id("25"),
        'val': "r"
    },
    {
        'disp': get_string_by_id("294"),
        'header': true,
        'dib': true,
    },
    {
        'disp': get_string_by_id("266"),
        'val': "n"
    },
    {
        'disp': get_string_by_id("267"),
        'val': "d"
    },
    {
        'disp': get_string_by_id("268"),
        'val': "i"
    },
    {
        'disp': get_string_by_id("269"),
        'val': "b"
    },
    {
        'disp': get_string_by_id("270"),
        'val': "s"
    }
        ]

        //Adjusts years when Number of Providers measure is chosen, which is only available from 2015 onward
    adjust_years = [{
        'disp': "2022",
        'val': "22",
        'optgroup': get_string_by_id("10")
    },{
        'disp': "2021",
        'val': "21",
        'optgroup': get_string_by_id("10")
    },
     {
        'disp': "2020",
        'val': "20",
        'optgroup': get_string_by_id("10")
    },
    {
        'disp': "2019",
        'val': "9",
        'optgroup': get_string_by_id("10")
    },
    {
        'disp': "2018",
        'val': "8",
        'optgroup': get_string_by_id("10")
    },
    {
        'disp': "2017",
        'val': "7",
        'optgroup': get_string_by_id("10")
    },
    {
        'disp': "2016",
        'val': "6",
        'optgroup': get_string_by_id("10")
    },
    {
        'disp': "2015",
        'val': "5",
        'optgroup': get_string_by_id("10")
    }]

    adjust_years2017 = [{
        'disp': "2022",
        'val': "22",
        'optgroup': get_string_by_id("10")
    },{
        'disp': "2021",
        'val': "21",
        'optgroup': get_string_by_id("10")
    },
     {
        'disp': "2020",
        'val': "20",
        'optgroup': get_string_by_id("10")
    },
    {
        'disp': "2019",
        'val': "9",
        'optgroup': get_string_by_id("10")
    },
    {
        'disp': "2018",
        'val': "8",
        'optgroup': get_string_by_id("10")
    },
    {
        'disp': "2017",
        'val': "7",
        'optgroup': get_string_by_id("10")
    }]


//Supresses "Other" race category from 2020 onward
race_edit = [{
    'disp': get_string_by_id("81"),
    'val': null
},
{
    'disp': get_string_by_id("82"),
    'val': "1"
},
{
    'disp': get_string_by_id("83"),
    'val': "2"
},
{
    'disp': get_string_by_id("85"),
    'val': "4"
},
{
    'disp': get_string_by_id("86"),
    'val': "5"
},
{
    'disp': get_string_by_id("87"),
    'val': "6"
}
    ];

    MA_years = [
        {
            'disp': "2018",
            'val': "8",
            'optgroup': get_string_by_id("10")
        },
        {
            'disp': "2019",
            'val': "9",
            'optgroup': get_string_by_id("10")
        },
       
        ];
    MA_measures = [
        {
            'disp': get_string_by_id("295"),
            'header': true,
            'dib': true,
        },
        /*{
            'disp': get_string_by_id("19"),
            'val': "e"
        },*/
        {
            'disp': get_string_by_id("20"),
            'val': "h"
        },/*
        {
            'disp': get_string_by_id("21"),
            'val': "m"
        },
        {
            'disp': get_string_by_id("22"),
            'val': "v"
        },
        {
            'disp': get_string_by_id("23"),
            'val': "q"
        },
        {
            'disp': get_string_by_id("24"),
            'val': "c"
        },
        {
            'disp': get_string_by_id("25"),
            'val': "r"
        },*/
        {
            'disp': get_string_by_id("294"),
            'header': true,
            'dib': true,
        },
        /*{
            'disp': get_string_by_id("266"),
            'val': "n"
        },*/
        {
            'disp': get_string_by_id("267"),
            'val': "d"
        },
        {
            'disp': get_string_by_id("268"),
            'val': "i"
        },
        /*{
            'disp': get_string_by_id("270"),
            'val': "s"
        }*/
        ];

    SDOH_measures = [
        {
            'disp': get_string_by_id("295"),
            'header': true,
            'dib': true,
        },
        {
            'disp': get_string_by_id("16"),
            'sel': true,
            'val': "p"
        },
        {
            'disp': get_string_by_id("17"),
            'val': "t"
        },
        {
            'disp': get_string_by_id("18"),
            'val': "a"
        },
        {
            'disp': get_string_by_id("19"),
            'val': "e"
        },
        {
            'disp': get_string_by_id("20"),
            'val': "h"
        },
        {
            'disp': get_string_by_id("21"),
            'val': "m"
        },
        {
            'disp': get_string_by_id("22"),
            'val': "v"
        },
        {
            'disp': get_string_by_id("23"),
            'val': "q"
        },
        {
            'disp': get_string_by_id("24"),
            'val': "c"
        },
        {
            'disp': get_string_by_id("25"),
            'val': "r"
        },
        {
            'disp': get_string_by_id("294"),
            'header': true,
            'dib': true,
        },
        {
            'disp': get_string_by_id("266"),
            'val': "n"
        },
        {
            'disp': get_string_by_id("267"),
            'val': "d"
        },
        {
            'disp': get_string_by_id("268"),
            'val': "i"
        },
        {
            'disp': get_string_by_id("269"),
            'val': "b"
        },
        {
            'disp': get_string_by_id("270"),
            'val': "s"
        },
        {
            'disp': get_string_by_id("317"),
            'header': true,
            'dib': true,
        },
        {
            'disp': get_string_by_id("318"),
            'val': "o"
        },
        {
            'disp': get_string_by_id("332"),
            'val': "u"
        },
        {
            'disp': get_string_by_id("333"),
            'header': true,
            'dib': true,
        },
        {
            'disp': get_string_by_id("334"),
            'val': "1"
        },
        {
            'disp': get_string_by_id("335"),
            'val': "2"
        },
        {
            'disp': get_string_by_id("336"),
            'val': "3"
        },
        {
            'disp': get_string_by_id("337"),
            'val': "4"
        },
        {
            'disp': get_string_by_id("338"),
            'val': "5"
        },
        {
            'disp': get_string_by_id("339"),
            'val': "6"
        },
        {
            'disp': get_string_by_id("340"),
            'val': "7"
        }
            ];

         

    SDOH_measures_state = [
        {
            'disp': get_string_by_id("295"),
            'header': true,
            'dib': true,
        },
        {
            'disp': get_string_by_id("16"),
            'sel': true,
            'val': "p"
        },
        {
            'disp': get_string_by_id("17"),
            'val': "t"
        },
        {
            'disp': get_string_by_id("18"),
            'val': "a"
        },
        {
            'disp': get_string_by_id("19"),
            'val': "e"
        },
        {
            'disp': get_string_by_id("20"),
            'val': "h"
        },
        {
            'disp': get_string_by_id("21"),
            'val': "m"
        },
        {
            'disp': get_string_by_id("22"),
            'val': "v"
        },
        {
            'disp': get_string_by_id("23"),
            'val': "q"
        },
        {
            'disp': get_string_by_id("24"),
            'val': "c"
        },
        {
            'disp': get_string_by_id("25"),
            'val': "r"
        },
        {
            'disp': get_string_by_id("294"),
            'header': true,
            'dib': true,
        },
        {
            'disp': get_string_by_id("266"),
            'val': "n"
        },
        {
            'disp': get_string_by_id("267"),
            'val': "d"
        },
        {
            'disp': get_string_by_id("268"),
            'val': "i"
        },
        {
            'disp': get_string_by_id("269"),
            'val': "b"
        },
        {
            'disp': get_string_by_id("270"),
            'val': "s"
        },
        {
            'disp': get_string_by_id("317"),
            'header': true,
            'dib': true,
        },
        {
            'disp': get_string_by_id("318"),
            'val': "o"
        },
        {
            'disp': get_string_by_id("332"),
            'val': "u"
        },
        {
            'disp': get_string_by_id("333"),
            'header': true,
            'dib': true,
        },
        {
            'disp': get_string_by_id("334"),
            'val': "1"
        },
        {
            'disp': get_string_by_id("335"),
            'val': "2"
        },
        {
            'disp': get_string_by_id("336"),
            'val': "3"
        },
        //{
        //    'disp': get_string_by_id("337"),
        //    'val': "k"
        //},
        {
            'disp': get_string_by_id("338"),
            'val': "5"
        },
        {
            'disp': get_string_by_id("339"),
            'val': "6"
        },
        {
            'disp': get_string_by_id("340"),
            'val': "7"
        }
            ];
        


        age_edit = [{
            'disp': get_string_by_id("71"),
            'val': null
        },
        {
            'disp': get_string_by_id("72"),
            'val': "0"
        },
        {
            'disp': get_string_by_id("328"),
            'val': "4",
            header: true
        },
        {
            'disp': get_string_by_id("73"),
            'val': "1"
        },
        {
            'disp': get_string_by_id("74"),
            'val': "2"
        },
        {
            'disp': get_string_by_id("75"),
            'val': "3"
        }
        ];
    

    /* Options/Menu */
    menus = [{
            'name': get_string_by_id("325"),
            'id': "population",
            'options': [{
                    'disp': get_string_by_id("326"),
                    'val': "f"
                },
                {
                    'disp': get_string_by_id("327"),
                    'val': "m"
                }
            ]},{
            'name': get_string_by_id("9"),
            'id': "year",
            'options': [
                 //Updated to include 2023
                 {
                    'disp': "2023",
                    'val': "23",
                    'optgroup': get_string_by_id("10")
                },
                 {
                    'disp': "2022",
                    'val': "22",
                    'optgroup': get_string_by_id("10")
                },
                 {
                    'disp': "2021",
                    'val': "21",
                    'optgroup': get_string_by_id("10")
                },
                 {
                    'disp': "2020",
                    'val': "20",
                    'optgroup': get_string_by_id("10")
                },
                {
                    'disp': "2019",
                    'val': "9",
                    'optgroup': get_string_by_id("10")
                },
                {
                    'disp': "2018",
                    'val': "8",
                    'optgroup': get_string_by_id("10")
                },
                {
                    'disp': "2017",
                    'val': "7",
                    'optgroup': get_string_by_id("10")
                },
                {
                    'disp': "2016",
                    'val': "6",
                    'optgroup': get_string_by_id("10")
                },
                {
                    'disp': "2015",
                    'val': "5",
                    'optgroup': get_string_by_id("10")
                },
                {
                    'disp': "2014",
                    'val': "4",
                    'optgroup': get_string_by_id("10")
                },
                {
                    'disp': "2013",
                    'val': "3",
                    'optgroup': get_string_by_id("10")
                },
                {
                    'disp': "2012",
                    'val': "2",
                    'optgroup': get_string_by_id("10")
                },
                //TODO: Make it dynamic? I guess the above "Single Year" years aren't dynamic, they're hard-coded,
                //so the MIN(Year) and MAX(Year) are likewise hard-coded
                //Convention for the val specifying a range, use the "->", hopefully unlikely to be seen in any other context
                {
                    'disp': "2012-2023",
                    'val': "2->23",
                    'optgroup': get_string_by_id("11")
                },
                {
                    'disp': "2012-2022",
                    'val': "2->22",
                    'optgroup': get_string_by_id("11")
                },
                {
                    'disp': "2012-2021",
                    'val': "2->21",
                    'optgroup': get_string_by_id("11")
                },
                {
                    'disp': "2012-2020",
                    'val': "2->20",
                    'optgroup': get_string_by_id("11")
                },
                {
                    'disp': "2012-2019",
                    'val': "2->9",
                    'optgroup': get_string_by_id("11")
                },
                {
                    'disp': "2012-2018",
                    'val': "2->8",
                    'optgroup': get_string_by_id("11")
                },
                {
                    'disp': "2012-2017",
                    'val': "2->7",
                    'optgroup': get_string_by_id("11")
                },
                {
                    'disp': "2012-2016",
                    'val': "2->6",
                    'optgroup': get_string_by_id("11")
                },
                {
                    'disp': "2012-2015",
                    'val': "2->5",
                    'optgroup': get_string_by_id("11")
                },
                {
                    'disp': "2012-2014",
                    'val': "2->4",
                    'optgroup': get_string_by_id("11")
                },
            ],
            //Everyone else returns falsey
            'grouped': true
        },
        {
            'name': get_string_by_id("12"),
            'id': "geography",
            'options': [{
                    'disp': get_string_by_id("13"),
                    'val': "c"
                },
                {
                    'disp': get_string_by_id("14"),
                    'val': "s"
                }
            ]
        },
        {
            'name': get_string_by_id("15"),
            'id': "measure",
            'options': [
                {
                    'disp': get_string_by_id("295"),
                    'header': true,
                    'dib': true,
                },
                {
                    'disp': get_string_by_id("16"),
                    'sel': true,
                    'val': "p"
                },
                {
                    'disp': get_string_by_id("17"),
                    'val': "t"
                },
                {
                    'disp': get_string_by_id("18"),
                    'val': "a"
                },
                {
                    'disp': get_string_by_id("19"),
                    'val': "e"
                },
                {
                    'disp': get_string_by_id("20"),
                    'val': "h"
                },
                {
                    'disp': get_string_by_id("21"),
                    'val': "m"
                },
                {
                    'disp': get_string_by_id("22"),
                    'val': "v"
                },
                {
                    'disp': get_string_by_id("23"),
                    'val': "q"
                },
                {
                    'disp': get_string_by_id("24"),
                    'val': "c"
                },
                {
                    'disp': get_string_by_id("25"),
                    'val': "r"
                },
                {
                    'disp': get_string_by_id("294"),
                    'header': true,
                    'dib': true,
                },
                {
                    'disp': get_string_by_id("266"),
                    'val': "n"
                },
                {
                    'disp': get_string_by_id("267"),
                    'val': "d"
                },
                {
                    'disp': get_string_by_id("268"),
                    'val': "i"
                },
                {
                    'disp': get_string_by_id("269"),
                    'val': "b"
                },
                {
                    'disp': get_string_by_id("270"),
                    'val': "s"
                },
                {
                    'disp': get_string_by_id("317"),
                    'header': true,
                    'dib': true,
                },
                {
                    'disp': get_string_by_id("318"),
                    'val': "o"
                },
                {
                    'disp': get_string_by_id("332"),
                    'val': "u"
                }
            ]
        },
        {
            'name': get_string_by_id("26"),
            'id': "adjust",
            'options': [{
                    'disp': get_string_by_id("27"),
                    'val': "1"
                },
                {
                    'disp': get_string_by_id("28"),
                    'val': "2"
                },
                {
                    'disp': get_string_by_id("29"),
                    'val': "3"
                },
                {
                    'disp': get_string_by_id("30"),
                    'val': "4"
                }
            ]
        },
        {
            'name': get_string_by_id("31"),
            'id': "analysis",
            'options': [{
                    'disp': get_string_by_id("32"),
                    'val': "base"
                },
                {
                    'disp': get_string_by_id("33"),
                    'val': "countydiff"
                },
                {
                    'disp': get_string_by_id("34"),
                    'val': "diffindiff"
                },
                {
                    'disp': get_string_by_id("35"),
                    'val': "statediff"
                },
                {
                    'disp': get_string_by_id("36"),
                    'val': "nationdiff"
                },
                {
                    'disp': get_string_by_id("37"),
                    'val': null,
                    'dib': null
                }
            ]
        },
        {
            'name': get_string_by_id("38"),
            'id': "domain",
            'options': [{
                    'disp': get_string_by_id("39"),
                    'val': "d1"
                },
                {
                    'disp': get_string_by_id("40"),
                    'val': "d2"
                },
                {
                    'disp': "Behavioral health conditions",
                    'val': "d5"
                },
                //{
                //    'disp': "COVID-19",
                //    'val': "d4"
                //},
                {
                    'disp': "Preventive Screenings / Immunizations",
                    'val':"d3"
                }
            ]
        },
        {
            'name': get_string_by_id("41"),
            'id': "condition",
            'options': [{
                    'disp': get_string_by_id("42"),
                    'val': "2"
                },
                {
                    'disp': get_string_by_id("43"),
                    'val': "1"
                },
                {
                    'disp': get_string_by_id("44"),
                    'val': "4"
                },
                {
                    'disp': get_string_by_id("45"),
                    'val': "3"
                },
                {
                    'disp': get_string_by_id("46"),
                    'val': "11"
                },
                {
                    'disp': get_string_by_id("47"),
                    'val': "5"
                },
                {
                    'disp': get_string_by_id("48"),
                    'val': "12"
                },
                {
                    'disp': get_string_by_id("49"),
                    'val': "13"
                },
                {
                    'disp': get_string_by_id("50"),
                    'val': "14"
                },
                {
                    'disp': get_string_by_id("51"),
                    'val': "15"
                },
                {
                    'disp': get_string_by_id("52"),
                    'val': "24"
                },
                {
                    'disp': get_string_by_id("53"),
                    'val': "25"
                },
                {
                    'disp': get_string_by_id("54"),
                    'val': "16"
                },
                {
                    'disp': get_string_by_id("55"),
                    'val': "18"
                },
                {
                    'disp': get_string_by_id("56"),
                    'val': "17"
                },
                {
                    'disp': get_string_by_id("57"),
                    'val': "19"
                },
                {
                    'disp': get_string_by_id("58"),
                    'val': "20"
                },
                {
                    'disp': get_string_by_id("59"),
                    'val': "21"
                },
                {
                    'disp': get_string_by_id("60"),
                    'val': "22"
                },
                {
                    'disp': get_string_by_id("61"),
                    'val': "23"
                },
                {
                    'disp': "   -----    ",
                    'val': null,
                    'dib': null
                },
                {
                    'disp': get_string_by_id("62"),
                    'val': "6"
                },
                {
                    'disp': get_string_by_id("63"),
                    'val': "7"
                },
                {
                    'disp': get_string_by_id("64"),
                    'val': "8"
                },
                {
                    'disp': get_string_by_id("65"),
                    'val': "9"
                }
            ]
        },
        {
            'name': get_string_by_id("66"),
            'id': "sex_code",
            'options': [{
                    'disp': get_string_by_id("67"),
                    'val': null
                },
                {
                    'disp': get_string_by_id("68"),
                    'val': "1"
                },
                {
                    'disp': get_string_by_id("69"),
                    'val': "2"
                }
            ]
        },
        {
            'name':get_string_by_id("70"),
            'id': "age_group",
            'options': [{
                    'disp': get_string_by_id("71"),
                    'val': null
                },
                {
                    'disp': get_string_by_id("72"),
                    'val': "0"
                },
                //{
                //    'disp': get_string_by_id("328"),
                //    'val': "4"
                //},
                {
                    'disp': get_string_by_id("73"),
                    'val': "1"
                },
                {
                    'disp': get_string_by_id("74"),
                    'val': "2"
                },
                {
                    'disp': get_string_by_id("75"),
                    'val': "3"
                }
            ]
        },
        {
            'name': get_string_by_id("80"),
            'id': "race_code",
            'options': [{
                    'disp': get_string_by_id("81"),
                    'val': null
                },
                {
                    'disp': get_string_by_id("82"),
                    'val': "1"
                },
                {
                    'disp': get_string_by_id("83"),
                    'val': "2"
                },
                {
                    'disp': get_string_by_id("84"),
                    'val': "3"
                },
                {
                    'disp': get_string_by_id("85"),
                    'val': "4"
                },
                {
                    'disp': get_string_by_id("86"),
                    'val': "5"
                },
                {
                    'disp': get_string_by_id("87"),
                    'val': "6"
                }
            ]
        },
        {
            'name': get_string_by_id("76"),
            'id': "dual",
            'options': [{
                    'disp': get_string_by_id("77"),
                    'val': null
                },
                {
                    'disp': get_string_by_id("78"),
                    'val': "0"
                },
                {
                    'disp': get_string_by_id("79"),
                    'val': "1"
                }
            ]
        },
        
        {
            'name': get_string_by_id("304"),
            'id': 'eligibility',
            'options': [
                {
                    'disp': get_string_by_id("81"),
                    'val': null,
                },
                {
                    'disp': get_string_by_id("300"),
                    'val': 0,
                },
                {
                    'disp': get_string_by_id("301"),
                    'val': 1,
                },
                {
                    'disp': get_string_by_id("302"),
                    'val': 2,
                },
                {
                    'disp': get_string_by_id("303"),
                    'val': 3,
                },
            ]
        },
        {
            'name': get_string_by_id(230),
            'id': "sex_code2",
            'options': [{
                'disp': get_string_by_id("67"),
                'val': null
            },
            {
                'disp': get_string_by_id("68"),
                'val': "1"
            },
            {
                'disp': get_string_by_id("69"),
                'val': "2"
            }
        ]
        },
        {
            'name': get_string_by_id(231),
            'id': "age_group2",
            'options': [{
                'disp': get_string_by_id("71"),
                'val': null
            },
            {
                'disp': get_string_by_id("72"),
                'val': "0"
            },
            //{
            //    'disp': get_string_by_id("328"),
            //    'val': "4"
            //},
            {
                'disp': get_string_by_id("73"),
                'val': "1"
            },
            {
                'disp': get_string_by_id("74"),
                'val': "2"
            },
            {
                'disp': get_string_by_id("75"),
                'val': "3"
            }
        ]
        },
        {
            'name': get_string_by_id("88"),
            'id': "race_code2",
            'options': [{
                'disp': get_string_by_id("81"),
                'val': null
            },
            {
                'disp': get_string_by_id("82"),
                'val': "1"
            },
            {
                'disp': get_string_by_id("83"),
                'val': "2"
            },
            {
                'disp': get_string_by_id("84"),
                'val': "3"
            },
            {
                'disp': get_string_by_id("85"),
                'val': "4"
            },
            {
                'disp': get_string_by_id("86"),
                'val': "5"
            },
            {
                'disp': get_string_by_id("87"),
                'val': "6"
            }
        ]
        },
        {
            'name': get_string_by_id(232),
            'id': "dual2",
            'options': [{
                'disp': get_string_by_id("77"),
                'val': null
            },
            {
                'disp': get_string_by_id("78"),
                'val': "0"
            },
            {
                'disp': get_string_by_id("79"),
                'val': "1"
            }
        ]
        },
        
        {
            'name': get_string_by_id("305"),
            'id': 'eligibility2',
            'options': [
                {
                    'disp': get_string_by_id("81"),
                    'val': null,
                },
                {
                    'disp': get_string_by_id("300"),
                    'val': 0,
                },
                {
                    'disp': get_string_by_id("301"),
                    'val': 1,
                },
                {
                    'disp': get_string_by_id("302"),
                    'val': 2,
                },
                {
                    'disp': get_string_by_id("303"),
                    'val': 3,
                },
            ]
        },
    ];

    if (callback !== undefined) {
        return callback();
    }
}



var get_year_label = function(val) {
    //Elsewhere it uses the actual select element to get the label
    //document.getElementById("measure").options[document.getElementById("measure").selectedIndex]

    var year_type = menus.filter(function(menu_type) {
        return menu_type.id === 'year';
    })[0];

    var year_option = year_type.options.filter(function(year_option) {
        return year_option.val === val;
    })

    var year = (year_option && year_option.length && year_option[0]);

    return year && year.disp;
}

function updateMenu(m, menu_data, grouped) {
     if (grouped) {

         //Use d3 to group-by
         var grouped_data = d3.nest()
             .key(function(d) {
                 return d.optgroup;
             })
             .entries(menu_data);

         //Data Join
         var optgroups = m.selectAll('optgroup')
             .data(grouped_data);

         //Enter
         optgroups
             .enter()
             .append('optgroup');

         //Enter_Update
         optgroups
             .attr('label', function(optgroup) {
                 //Key is the value of the optgroup property; serves as its label
                 return optgroup.key;
             });

         //Nested select
         var opts = optgroups
             .selectAll('option')
             .data(function(optgroup) {
                 //Values is an array of options that share the same optgroup property
                 return optgroup.values;
             });

         //Exit
         optgroups
             .exit()
             .remove();

     } else {

        // Data Join
        var opts = m.selectAll("option")
            .data(menu_data);
    }

    // Enter
    opts.enter().append("option");

    // Enter+Update
    opts
        .attr("value", function(d) {
            return d.val === null ? "null" : d.val;
        })
        .attr("selected", function(d) {
            return ('sel' in d) ? true : null
        })
        .attr("disabled", function(d) {
            return ('dib' in d) ? true : null
        })
        .attr("class", function(d) {
            if ('header' in d) { return d.header ? 'opt-header' : 'opt-group' }
        })
        .text(function(d) {
            return d.disp;
        });

    // Exit
    opts.exit().remove();

}

var menu_order;
function initialize_menus() {
    menu_order = {};

    menus.forEach(function(d, i) {
        menu_order[d.id] = i;
    });

    var menu_container_div = d3.select("section#mmd-map-container")
        .append("div")
        .attr("id", "menu-container");

    menu_container_div
      .append("div")
      .attr("id", "menu-container1");

      var menu_container = menu_container_div
        .append("table")
        .attr("role", "presentation");

    menus.forEach(function(d) {
        var select_container = menu_container
            .append("tr");

        select_container
            .append("td")
            .append("label")
            .attr("for", d.id)
            .text(d.name);

        var menu = select_container.append("td")
            .append("select")
            .attr("id", d.id)
            .on("change", function() {
                //BUG Potential: Must change the tooltip BEFORE loading the trend view
                //So that the tooltip() function will change the DOM *before* the load_trend_view looks in the DOM to find it

                //https://jira.us.kworld.kpmg.com/secure/RapidBoard.jspa?rapidView=398&projectKey=NORCCMS&view=planning&selectedIssue=NORCCMS-26
                tooltip(last_tooltiped_jurisdiction, menuChange(), "over");

                if (d.id !== "geography" && d_map_container.classed('view-showing') && d_trend_view.classed('showing')){
                    load_trend_view();
                }

                if (d.id === "geography") {
                    map_maximize(false);
                    pin_tooltip(false, undefined, undefined);
                    d_map_container.classed({'view-showing': false});
                }

                var opts = menuChange();

                populate(opts);

            });

        updateMenu(menu, d.options, d.grouped);

    });
}

function get_condition_name() {
    ms=document.getElementById("measure").options[document.getElementById("measure").selectedIndex].value
    dm=document.getElementById("domain").options[document.getElementById("domain").selectedIndex].value
    if (ms=== "1"|| ms=== "2" || ms=== "3" || ms=== "4" || ms=== "5" || ms=== "6" || ms=== "7"){
        var condition_name=""
    }
    else if (dm==='d4'){
        var condition_name="COVID-19"
    }
    else{
        var condition_name = document
        .getElementById("condition")
        .options[document.getElementById("condition")
        .selectedIndex].text;

    }
  
  return condition_name;
}

function get_measure_name(){

  //TODO: Code to get pretty label is repeated twice. Once in the legend label in map.html Use a function.
  var measure_name = document
                      .getElementById("measure")
                      .options[document.getElementById("measure")
                      .selectedIndex].text;

  if (measure_name === get_string_by_id("23")) {
      measure_name = "PQI";
  }

  return measure_name;

}

function get_race_text(r){
    r = r || 'race_code';
    return document
      .getElementById(r)
      .options[document.getElementById(r).selectedIndex]
      .text;
}

function get_dual_text(du){
    du = du || 'dual';
    return document
      .getElementById(du)
      .options[document.getElementById(du).selectedIndex]
      .text;
}
function get_age_text(ag){
    ag = ag || 'age_group';
    return document
      .getElementById(ag)
      .options[document.getElementById(ag).selectedIndex]
      .text;
}
function get_sexcode_text(sc){
    sc = sc || 'sex_code';
    return document
      .getElementById(sc)
      .options[document.getElementById(sc).selectedIndex]
      .text;
}

function restrict_available_years(years, reset, revert_to) {

    revert_to = reset || revert_to || years[0];
    reset = reset || false;

    var isinlist = function(list, d) {
        var isinlist = false;
        list.forEach(function(elem) { if (d === elem) { isinlist = true; } });
        return isinlist;
    };

    var selectedYear = document.getElementById("year").value;

    if (!reset && !isinlist(years, selectedYear) && isinlist(years,revert_to)) {
        var i = 0;
        while (document.getElementById("year").options[i].value !== revert_to) {
            i++;
        }
        document.getElementById("year").selectedIndex = i;
    }

    var year_options = document.getElementById("year").options;
    var l = year_options.length;
    for (var i = 0; i < l; i++) {
        var d = year_options[i];
        if (reset || isinlist(years,d.value)) { d.disabled = false; }
        else { d.disabled = true; }
    }
}

function filterConditions(domain, measure) {
    return conditions.filter(function(d) {
        return d.domain === domain &&
               d.measure.indexOf(measure) !== -1;
        });
}

function filterBHConditions(BHConditions, domain, measure) {
    //console.log(BHConditions.filter(function(d) { 
    //    
    //           d.measure.indexOf(measure)
    //    }))
    //console.log(domain)
    //console.log(measure)
    return BHConditions.filter(function(d) { 
        return d.domain === domain &&
               d.measure.indexOf(measure) !== -1;
        });
}

var PREVIOUS_D_M = {'d': 1, 'm': 'v'};
function toggleDomain(domain, measure) {
    //console.log("Toggle Domain"+ domain +" "+ measure)
    //console.log(filterConditions(domain,measure))
    if (PREVIOUS_D_M.d !== domain || PREVIOUS_D_M.m !== measure) {
        updateMenu(d3.select("#condition"), filterConditions(domain,measure));
        PREVIOUS_D_M.d = domain;
        PREVIOUS_D_M.m = measure;
    }
}

function resetConditions() {
    document.getElementById("condition").selectedIndex = 0;
    document.getElementById("condition").value = document.getElementById("condition").options[0].value;
}

function menuChange() {

    //No ned to hide it
    // d_tooltip_container.classed({
    //     'showing': false
    // });
    //console.log(document.getElementById("population").value);
    var tp = document.getElementById("population").value;
    var ms = document.getElementById("measure").value;
    var ds = document.getElementById("domain").value;
    var yr = document.getElementById("year").value;
    var cond = document.getElementById("condition").value;


    document.getElementById("population").disabled = false;
    document.getElementById("dual").disabled = false;
    document.getElementById("dual2").disabled = false;
    document.getElementById("eligibility").disabled = false;
    document.getElementById("eligibility2").disabled = false;

   //Toggle to MA menu options
    if (tp==="m"){
        updateMenu(d3.select("#year"), MA_years, true);
        updateMenu(d3.select("#measure"), MA_measures);
        if (document.getElementById("measure").selectedIndex === 2){
            document.getElementById("measure").selectedIndex = 1;
        }
        updateMenu(d3.select("#domain"), MA_domains);
        document.getElementById("geography").value='s';
        document.getElementById("geography").disabled = true;
        updateMenu(d3.select("#analysis"), MA_analyses);
        updateMenu(d3.select("#age_group"), age_edit);
        updateMenu(d3.select("#age_group2"), age_edit);
        
    }
    else if (tp==="f"){
        updateMenu(d3.select("#year"), menus[menu_order['year']].options, true);
        //updateMenu(d3.select("#measure"), menus[menu_order['measure']].options);
        updateMenu(d3.select("#domain"), menus[menu_order['domain']].options);
        document.getElementById("geography").disabled = false;
        if (yr==21 || yr==22|| yr==23){
            //Updates age group for years 2021+
            updateMenu(d3.select("#age_group"), age_edit);
            updateMenu(d3.select("#age_group2"), age_edit);
        }
        else{
            updateMenu(d3.select("#age_group"), menus[menu_order['age_group']].options);
            updateMenu(d3.select("#age_group2"), menus[menu_order['age_group2']].options);
        }
        //updateMenu(d3.select("geography"), menus[menu_order['geography']].options, true);
        //document.getElementById("geography").value='c';
        //document.getElementById("domain").value = document.getElementById("domain").options[0].value;
       // updateMenu(d3.select("#condition"), menus[menu_order['condition']].options);
        //document.getElementById("condition").value = document.getElementById("condition").options[0].value;

    }
     /* Update for Domain selections and conditions*/
    if(yr==20){
        updateMenu(d3.select("#race_code"), race_edit);
        updateMenu(d3.select("#race_code2"), race_edit);
        document.getElementById("domain").disabled = false;
        document.getElementById("condition").disabled = false; 
        if( ms === "v" ){
            updateMenu(d3.select("#domain"),adjust_cond_domains);
            document.getElementById("domain").disabled = false;
          }else if(ms === "h" ){
            updateMenu(d3.select("#domain"),adjust_cond_domains);
          document.getElementById("domain").disabled = false;
          }else if(ms === "p" ){
          //updateMenu(d3.select("#domain"),adjust_cond_domains);
          document.getElementById("domain").disabled = false;
          }else if(ms === "t" ){
          //updateMenu(d3.select("#domain"),adjust_cond_domains);
          document.getElementById("domain").disabled = false;
          }else if(ms === "a" ){
          //updateMenu(d3.select("#domain"),adjust_cond_domains);
          document.getElementById("domain").disabled = false;
          }else if(ms === "e" ){
              updateMenu(d3.select("#domain"),cond_domains);
              document.getElementById("domain").disabled = false;
            }else if(ms === "c" ){
                updateMenu(d3.select("#domain"),preventive_domains);
                document.getElementById("domain").disabled = true;
            }
            else if(ms === "o"|| ms === "u" ){
                document.getElementById("domain").disabled = true;  
            }else if (ms=== "1"|| ms=== "2" || ms=== "3" || ms=== "4" || ms=== "5" || ms=== "6" || ms=== "7"){
                document.getElementById("population").disabled = true;
                //SDOH
                if (ms=== "4"){
                    document.getElementById("geography").disabled = true;
                    document.getElementById("geography").selectedIndex = 0
                }
                document.getElementById("analysis").selectedIndex = 0   
                document.getElementById("domain").disabled = true;
                document.getElementById("domain").selectedIndex = 0  
                //updateMenu(d3.select("#domain"), MA_domains);
                document.getElementById("condition").disabled = true; 
                document.getElementById("domain").disabled = true;  
                document.getElementById("sex_code").disabled = true
                document.getElementById("sex_code").value = 'null'
                document.getElementById("age_group").disabled = true
                document.getElementById("age_group").value = 'null'
                document.getElementById("race_code").disabled = true
                document.getElementById("race_code").value = 'null'
                document.getElementById("dual").disabled = true
                document.getElementById("dual").value = 'null'
                document.getElementById("eligibility").disabled = true
                document.getElementById("eligibility").value = 'null'
            }
          else{
            updateMenu(d3.select("#domain"),other_domains);
          document.getElementById("domain").disabled = true;
          }

        }
        else if (yr=="2->20" || yr=="2->21" || yr=="2->22"){ 
            updateMenu(d3.select("#race_code"), race_edit);
            updateMenu(d3.select("#race_code2"), race_edit);
            updateMenu(d3.select("#domain"),cond_domains);
            //toggleDomain(_dsel, ms);
    }
    else if (yr>20){
        updateMenu(d3.select("#race_code"), race_edit);
        updateMenu(d3.select("#race_code2"), race_edit);
        document.getElementById("condition").disabled = false;
    }
    else{
        updateMenu(d3.select("#race_code"), menus[menu_order['race_code']].options);
        updateMenu(d3.select("#race_code2"), menus[menu_order['race_code2']].options);
        //toggleDomain(_dsel, ms);
        if (tp==="m"){
            document.getElementById("domain").disabled = true;
            updateMenu(d3.select("#race_code"), race_edit);
            if (ms ==="h"){
                //Hospitalization
                updateMenu(d3.select("#domain"),MA_domains);
                //updateMenu(d3.select("#domain"), menus[menu_order['domain']].options);
                updateMenu(d3.select("#condition"), MA_conditions.filter(function(d) {
                    var filteredCondition = d.measure.indexOf(ms) > -1;
                    return filteredCondition;
                }));
                //updateMenu(d3.select("#condition"),MA_conditions);
                document.getElementById("condition").disabled = true;
            }
            else if (ms ==="i"){
                updateMenu(d3.select("#domain"), menus[menu_order['domain']].options);
                updateMenu(d3.select("#condition"), conditions.filter(function(d) {
                    var filteredCondition = d.measure.indexOf(ms) > -1;
                    return filteredCondition;
                }));
                //updateMenu(d3.select("#condition"),MA_conditions);
                document.getElementById("condition").disabled = true;
            }
            else{
                //Discharge destination
                updateMenu(d3.select("#domain"),MA_domains);
                updateMenu(d3.select("#condition"),MA_conditions_discharge);
                document.getElementById("condition").disabled = false;}
        }else if( ms === "v" ){
            updateMenu(d3.select("#domain"),cond_domains);
            document.getElementById("domain").disabled = false;
            document.getElementById("condition").disabled = false;
        }else if(ms === "h" ){
            updateMenu(d3.select("#domain"),cond_domains);
            document.getElementById("domain").disabled = false;
            document.getElementById("condition").disabled = false;
        }else if(ms === "p" ){
            updateMenu(d3.select("#domain"),cond_domains);
            document.getElementById("domain").disabled = false;
            document.getElementById("condition").disabled = false;
        }else if(ms === "t" ){
            updateMenu(d3.select("#domain"),cond_domains);
            document.getElementById("domain").disabled = false;
            document.getElementById("condition").disabled = false;
        }else if(ms === "a" ){
            updateMenu(d3.select("#domain"),cond_domains);
            document.getElementById("domain").disabled = false;
            document.getElementById("condition").disabled = false;
        }
        else if(ms === "e" ){
            updateMenu(d3.select("#domain"),cond_domains);
            document.getElementById("domain").disabled = false;
            document.getElementById("condition").disabled = false;
        }
        else if(ms === "c" ){
            updateMenu(d3.select("#domain"),preventive_domains);
            document.getElementById("domain").disabled = true;
            document.getElementById("condition").disabled = false;
        }
        else if(ms === "o" || ms === "u" ){
            document.getElementById("domain").disabled = true;  
            document.getElementById("condition").disabled = false;
        }
        else{
            updateMenu(d3.select("#domain"),other_domains);
            document.getElementById("domain").disabled = true;
            document.getElementById("condition").disabled = false;
        }

    }
    /* Update for State Selection */
    if (document.getElementById("geography").value == "s") {
        document.getElementById("county_dd").disabled = true;
        selectedOption = document.getElementById("analysis").selectedIndex;
        d3.select("#urbanoverlay").style("opacity", 0);
        //Resets menu if selected option is not available for state geography
        if (selectedOption === 2 || selectedOption === 3){
            document.getElementById("analysis").selectedIndex = 0  
        } 
        if (tp==='f'){
            updateMenu(d3.select("#analysis"), state_analyses);
            if (ms==="o" || ms === "u"){
                document.getElementById("domain").disabled = true;
                document.getElementById("analysis").disabled = true;
                if (cond==='143'){
                    document.getElementById("analysis").disabled = true;
                    updateMenu(d3.select("#year"), adjust_years2017, true);
                }
                else{
                    document.getElementById("analysis").disabled = true;
                    updateMenu(d3.select("#year"), adjust_years, true);
                    //updateMenu(d3.select("#analysis"), market_metrics_analyses);
                }          
            }
            else if (ms=== "1"|| ms=== "2" || ms=== "3" || ms=== "4" || ms=== "5" || ms=== "6" || ms=== "7"){
                document.getElementById("domain").disabled = true;
                document.getElementById("analysis").disabled = true;
                document.getElementById("condition").disabled = true;

            }
            else if (ms=== "n"|| ms=== "d" || ms=== "i"|| ms=== "b"|| ms=== "s"|| ms=== "m"|| ms=== "r"|| ms=== "q"|| ms=== "c"){
                document.getElementById("domain").disabled = true;
            }
            else{
                document.getElementById("analysis").disabled = false;
                document.getElementById("domain").disabled = false;
            }
        }
        else {
            //Resets analysis menu for MA population when urban rural analysis is selected
            if (document.getElementById("analysis").selectedIndex===4){
                document.getElementById("analysis").selectedIndex = 0
            }
        }
    } else {
        if (tp==="f"){
            document.getElementById("county_dd").disabled = false;
            updateMenu(d3.select("#analysis"), menus[menu_order['analysis']].options);
            if (document.getElementById("analysis").selectedIndex === 5){
                document.getElementById("analysis").selectedIndex = 4 
            }
            if (ms==="o" || ms === "u"){
                document.getElementById("domain").disabled = true;
                if (cond==='143'){
                    document.getElementById("analysis").disabled = true;
                    updateMenu(d3.select("#year"), adjust_years2017, true);
                }
                else{
                    document.getElementById("analysis").disabled = true;
                    updateMenu(d3.select("#year"), adjust_years, true);
                    //updateMenu(d3.select("#analysis"), market_metrics_analyses);
                }  
            }
        else if ((ms=== "1"|| ms=== "2" || ms=== "3" || ms=== "4" || ms=== "5" || ms=== "6" || ms=== "7")){
            document.getElementById("analysis").disabled = true;
        }
        else if (ms==="m" || ms=== "r" || ms=== "q" || ms=== "n" || ms=== "d" || ms=== "i" || ms=== "b" || ms=== "s"){
            document.getElementById("domain").disabled = true;
            document.getElementById("analysis").disabled = false;
            
        }
        else if (ms==="c"){
            document.getElementById("domain").disabled = true;
        }
        else{
        document.getElementById("domain").disabled = false;
        document.getElementById("analysis").disabled = false;
        updateMenu(d3.select("#analysis"), menus[menu_order['analysis']].options);
        updateMenu(d3.select("#year"), menus[menu_order['year']].options, true);
        //initialize_menus();
        //updateMenu(menu, d.options, d.grouped);
        }}
        d3.select("#urbanoverlay").style("opacity", null);
    };

    
    /* Update Adjustment menu based on measure selection */
    // Additionally, disable smoothing is measure is heart failure
    // for the hospitalizaton measure, as well as for inpatient days and medicare reimbursement
    if (
        (ms == "p" || ms == "a" || ms == "t" || ms == "i" || ms == "b" ) //||
        //(yr == "9" || yr == "2->9") ||
      // (
       //    (ms == "h") &&
       //    (cond == "16")
        //)
    ) {
        updateMenu(d3.select("#adjust"), adjust_unsmooth_suppress);
        document.getElementById("adjust").disabled = false;
    }else if (ms == "n" || ms == "d") {
        updateMenu(d3.select("#adjust"),  adjustment_unsmoothed_only);
        document.getElementById("adjust").disabled = true;
    } 
    else if (ms=== "1"|| ms=== "2" || ms=== "3" || ms=== "4" || ms=== "5" || ms=== "6" || ms=== "7"){
        document.getElementById("adjust").disabled = true;
    }
    else {
        updateMenu(d3.select("#adjust"), menus[menu_order['adjust']].options) 
        document.getElementById("adjust").disabled = false;
    }

    //Disable inpatient measures for 2021 and add COVID-19 condition if measure is hospitalization or prevalence
    if ((yr=="20" || yr=="21" || yr=="22"|| yr=="23") && tp==="f") {
        updateMenu(d3.select("#domain"), adjust_cond_domains);
        //Adds COVID-19 domain in 2020, 2021, and 2022
        if(ds=='d4'){
            updateMenu(d3.select("#measure"), measure_covid);
            //console.log(adjust_conditions)
            updateMenu(d3.select("#condition"), adjust_conditions);
            //Provides only COVID-19 condition when COVID-19 domain is selected
            //if (ms=="h" || ms=="v"){
            //    updateMenu(d3.select("#condition"), adjust_conditions);
            //}
        }
        else if (ds=='d5' && (yr=="21"||yr=="22"||yr=="23")){
            //Supressed inpatient measures
            if (yr=="23"){
                updateMenu(d3.select("#measure"), measure_inpatient_suppress);
            }
            else{
                updateMenu(d3.select("#measure"), menus[menu_order['measure']].options)   
            }
            //TO DO: Need to fix filtering of BH conditions by measure in 2021
            updateMenu(d3.select("#condition"), BH_conditions.filter(function(d) {
                var filteredCondition = d.measure.indexOf(ms) > -1;
                return filteredCondition;
            }));
            if (ms=="c" || ms=="m" || ms=="q" || ms=="r" || ms=="n" || ms=="d" || ms=="i" || ms=="b" || ms=="s" || ms=="o" || ms=="u"){
                //TO DO: Finish and duplicate for other years
                document.getElementById("domain").selectedIndex = 0; 
                ds=1;
                updateMenu(d3.select("#condition"), conditions.filter(function(d) {
                    var filteredCondition = d.measure.indexOf(ms) > -1;
                    return filteredCondition;
                }));
            }
            //console.log(BH_conditions)
            //updateMenu(d3.select("#condition"), menus[menu_order['BH_conditions']].options)
            //updateMenu(d3.select("#condition"), filterBHConditions(BH_conditions, ds,ms));
            //updateMenu(d3.select("#condition"), BH_conditions.options);
        }
        else if (ds=='d5'){
            //console.log("Test")
            updateMenu(d3.select("#measure"), menus[menu_order['measure']].options)
            updateMenu(d3.select("#condition"), BH_conditions_pre2021.filter(function(d) {
                var filteredCondition = d.measure.indexOf(ms) > -1;
                return filteredCondition;
            }));
        //    //toggleDomain(_dsel, ms);
        //    updateMenu(d3.select("#condition"), menus[menu_order['conditions']].options);
        }
        else{
            if (document.getElementById("condition").selectedIndex === 1 && ms=== "e"){
                document.getElementById("condition").selectedIndex = 0 
            }
           // updateMenu(d3.select("#condition"), menus[menu_order['condition']].options)
            //toggleDomain(_dsel, ms);
            
            if (yr=="23"){   
                updateMenu(d3.select("#measure"), measure_inpatient_suppress);
                //updateMenu(d3.select("#measure"), menus[menu_order['measure']].options)
            }
            else if (yr=="21" || yr=="22"){
                updateMenu(d3.select("#measure"), menus[menu_order['measure']].options)
                //updateMenu(d3.select("#measure"), measure_inpatient_suppress);
            }
            else if (yr=="20"){
                //updateMenu(d3.select("#measure"), menus[menu_order['measure']].options)
                if (document.getElementById("geography").value == "s"){
                    updateMenu(d3.select("#measure"), SDOH_measures_state);
                }
                else {
                    updateMenu(d3.select("#measure"), SDOH_measures);
            }
                
                /*updateMenu(d3.select("#condition"), conditions.filter(function(d) {
                    var filteredCondition = d.measure.indexOf(ms) > -1;
                    return filteredCondition;
                }));*/
            }
            else{
                updateMenu(d3.select("#measure"), menus[menu_order['measure']].options)
            }
        }
        //Supresses inpatient measures for 2012-2022 trend 
    } else if (yr=="2->21" && tp==="f"){
        updateMenu(d3.select("#measure"), menus[menu_order['measure']].options)
        //updateMenu(d3.select("#condition"), menus[menu_order['condition']].options);
       //updateMenu(d3.select("#measure"), measure_inpatient_suppress);
       if (ds=='d5'){
        updateMenu(d3.select("#condition"), BH_conditions_pre2021.filter(function(d) {
            var filteredCondition = d.measure.indexOf(ms) > -1;
            return filteredCondition;
        }));
       }
    } 
    else if (yr=="2->22" && tp==="f"){
        updateMenu(d3.select("#measure"), menus[menu_order['measure']].options)
        //updateMenu(d3.select("#condition"), menus[menu_order['condition']].options);
       //updateMenu(d3.select("#measure"), measure_inpatient_suppress);
    } 
    else if (yr=="2->23" && tp==="f"){
        updateMenu(d3.select("#measure"), menus[menu_order['measure']].options)
        //updateMenu(d3.select("#condition"), menus[menu_order['condition']].options);
       updateMenu(d3.select("#measure"), measure_inpatient_suppress);
    } 
    else if ((yr==2|| yr==3 || yr==4 || yr.includes("->")) && tp==="f") {
        updateMenu(d3.select("#measure"), measure_pre2014);
        if (ds=='d5'){
            updateMenu(d3.select("#condition"), BH_conditions_pre2021.filter(function(d) {
                var filteredCondition = d.measure.indexOf(ms) > -1;
                return filteredCondition;
            }));
           }
    }
    else if (tp==="f") {
        updateMenu(d3.select("#measure"), menus[menu_order['measure']].options)
        if (ds=='d5'){
            updateMenu(d3.select("#condition"), BH_conditions_pre2021.filter(function(d) {
                var filteredCondition = d.measure.indexOf(ms) > -1;
                return filteredCondition;
            }));
           }
    } else{
        updateMenu(d3.select("#measure"), MA_measures)
    }
//if (ds=='d4'){
//    console.log("test")
//    updateMenu(d3.select("#measure"), measure_covid);
//    updateMenu(d3.select("#condition"), adjust_conditions);
//}
    /* Update condition menu based on measure selection */
    document.getElementById("sex_code").disabled = false;
    document.getElementById("race_code").disabled = false;
    if (ms === "q") {
        ds = 'd0';
        document.getElementById("dual").disabled = true;

        document.getElementById("dual").selectedIndex = 0;
        document.getElementById("dual2").disabled = true;
        document.getElementById("dual2").selectedIndex = 0;       
    } else if (ms === "h") {
        //If ESRD/DIB then Medicare eligibility is disabled. Applies only to Prevalence/Hospitalization/ER Visits
        if (cond==="24" || cond==="25"){
            document.getElementById("eligibility").disabled = true;
        }
        else{
            document.getElementById("eligibility").disabled = false;
        }
    } else if (ms === "e") {
        //If ESRD/DIB then Medicare eligibility is disabled. Applies only to Prevalence/Hospitalization/ER Visits
        if (cond==="24" || cond==="25"){
            document.getElementById("eligibility").disabled = true;
        }
        else{
            document.getElementById("eligibility").disabled = false;
        }
       // ds = 'd1';
    } else if (ms === "p") {
    } else if (ms === "m") {
        ds = 'd1';
        document.getElementById("dual").disabled = true;
        document.getElementById("dual").selectedIndex = 0;
        document.getElementById("dual2").disabled = true;
        document.getElementById("dual2").selectedIndex = 0;
        
    } else if (ms === "r") {
        ds = 'd1';
        document.getElementById("dual").disabled = true;
        document.getElementById("dual").selectedIndex = 0;
        document.getElementById("dual2").disabled = true;
        document.getElementById("dual2").selectedIndex = 0;
        if (document.getElementById("condition").selectedIndex == 1){
            document.getElementById("condition").selectedIndex = 0;
        }
        
    } else if (ms === "t" ) {
        //Resets condition menu selection when "---" option is selected when switching domains
        if (ds == "d1" & document.getElementById("condition").selectedIndex == 24){
            document.getElementById("condition").selectedIndex = 0;
        }
    } else if(ms === "a"){
    } else if(ms === "v"){
        //If ESRD/DIB then Medicare eligibility is disabled. Applies only to Prevalence/Hospitalization/ER Visits
        if (cond==="24" || cond==="25"){
            document.getElementById("eligibility").disabled = true;
        }
        else{
            document.getElementById("eligibility").disabled = false;
        }
    } else if (ms==="s"){
        ds = 'd3';
        document.getElementById("dual").disabled = true;
        document.getElementById("dual").selectedIndex = 0;
        document.getElementById("dual2").disabled = true;
        document.getElementById("dual2").selectedIndex = 0;

    } else if(ms === "c" || ms==="n" || ms==="d" ||ms==="i"|| ms==="b"){
        ds = 'd3';
    } 
    else if (ms === "o" || ms === "u"){
        //Market saturation measures
        ds = 'd0';
        document.getElementById("analysis").value = 'base';
        document.getElementById("adjust").selectedIndex = 0;
        document.getElementById("adjust").disabled = true;
        document.getElementById("sex_code").disabled = true;
        document.getElementById("sex_code").selectedIndex = 0;
        document.getElementById("age_group").disabled = true;
        document.getElementById("age_group").selectedIndex = 0;
        document.getElementById("race_code").disabled = true;
        document.getElementById("race_code").selectedIndex = 0;
        document.getElementById("dual").disabled = true;
        document.getElementById("dual").selectedIndex = 0;
        document.getElementById("eligibility").disabled = true;
        document.getElementById("eligibility").selectedIndex = 0;
    }
    else if (ms=== "1"|| ms=== "2" || ms=== "3" || ms=== "4" || ms=== "5" || ms=== "6" || ms=== "7"){
        //SDOH measures
        ds = 'd0';
        document.getElementById("age_group").disabled = true;
        document.getElementById("sex_code").disabled = true;
        document.getElementById("race_code").disabled = true;
    }
    else {
        updateMenu(d3.select("#condition"), menus[menu_order['condition']].options);
        updateMenu(d3.select("#race_code"), menus[menu_order['race_code']].options);
        updateMenu(d3.select("#race_code2"), menus[menu_order['race_code2']].options);
    };
    /* MASTER SECTION to update Conditions */
    var _dsel = {'d1': 1, 'd2': 2, 'd3': 3, 'd5': 5, 'd0': 0, 'null':null}[ds];
    //console.log(_dsel)
    //console.log(ms)
    if (tp!== "m"){toggleDomain(_dsel, ms);}
    //Adds or removes 10 primary cronic conditions for 2022 or later
    if (ds=='d1' && (yr!=="22" && yr!=="23")){
        updateMenu(d3.select("#condition"), conditions.filter(function(d) {
            var pqi = d.measure.indexOf(ms) > -1;
            var allowedDomain = (d.domain == '1');
            var allowed = (d.val !== '147' && d.val !== '148' && d.val !== '149' && d.val !== '150' && d.val !== '151' && d.val !== '152' && d.val !== '153' && d.val !== '154'&& d.val !== '155' && d.val !== '156');

            return pqi && allowedDomain && allowed;
        }));
    }
    else if (ds=='d1'){
        updateMenu(d3.select("#condition"), conditions.filter(function(d) {
            var pqi = d.measure.indexOf(ms) > -1;
            var allowedDomain = (d.domain == '1');

            return pqi && allowedDomain;
        }));
    }
    //Adds or removes Sickle Cell Disease to conditions list
    if (ds=='d2' && (yr!=="22" && yr!=="23")){
        updateMenu(d3.select("#condition"), conditions.filter(function(d) {
            var pqi = d.measure.indexOf(ms) > -1;
            var allowedDomain = (d.domain == '2');
            var allowed = (d.val !== '144');

            return pqi && allowedDomain && allowed;
        }));
    }
    else if (ds=='d2'){
        updateMenu(d3.select("#condition"), conditions.filter(function(d) {
            var pqi = d.measure.indexOf(ms) > -1;
            var allowedDomain = (d.domain == '2');
            //var allowed = (d.val !== '144');

            return pqi && allowedDomain;
        }));
        //When Sickle Cell Disease is selected, sets menu to show only state-level data
        if (document.getElementById("condition").value==='144'){
            document.getElementById("geography").value = 's';
            document.getElementById("geography").disabled=true;
        }
    }
    // 27, 32
    // Disable PQI 2, PQI 10 if year is 2019 or greater
    if ((ms === "q") && (yr === "9" || yr === "20"|| yr === "21" || yr === "22")) {
        updateMenu(d3.select("#condition"), conditions.filter(function(d) {
            var pqi = d.measure.indexOf('q') > -1;
            var allowed = (d.val !== '27') && (d.val !== '32') && (d.val !== '35');

            return pqi && allowed;
        }));
    }
    //Disables PQI 13 after 2015
    else if (((ms === "q") && (yr === "6" || yr === "7"|| yr === "8" || yr === "2->6" || yr === "2->7" || yr === "2->8" || yr === "2->9" || yr === "2->20" || yr === "2->21"|| yr === "2->22"))){
        updateMenu(d3.select("#condition"), conditions.filter(function(d) {
            var pqi = d.measure.indexOf('q') > -1;
            var allowed = (d.val !== '35');

            return pqi && allowed;
        }));
    }
    else if (ms === "q") {
        updateMenu(d3.select("#condition"), conditions.filter(function(d) {
            var pqi = d.measure.indexOf('q') > -1;

            return pqi;
        }));
    }

    /* Second Race Selection Available only for non-base analysis */
    if (document.getElementById("analysis").value !== "base") {
        document.getElementById("race_code2").disabled = false;
        document.getElementById("age_group2").disabled = false;
        document.getElementById("sex_code2").disabled = false;
        document.getElementById("eligibility2").disabled = false;
        if( ms === "m" || ms === "r" ||  ms === "q" ||  ms === "s"){
            document.getElementById("dual2").disabled = true;
        }
        else if (ms === "h") {
            //If ESRD/DIB then Medicare eligibility is disabled. Applies only to Prevalence/Hospitalization/ER Visits
            if (cond==="24" || cond==="25"){
                document.getElementById("eligibility2").disabled = true;
            }}
        else if(ms === "v"){
            //If ESRD/DIB then Medicare eligibility is disabled. Applies only to Prevalence/Hospitalization/ER Visits
            if (cond==="24" || cond==="25"){
                document.getElementById("eligibility2").disabled = true;
            }
        }
        else{
            document.getElementById("dual2").disabled = false;
        }
        
    } else {
        document.getElementById("race_code2").disabled = true;
        document.getElementById("race_code2").selectedIndex = 0;
        document.getElementById("dual2").disabled = true;
        document.getElementById("dual2").selectedIndex = 0;
        document.getElementById("age_group2").disabled = true;
        document.getElementById("age_group2").selectedIndex = 0;
        document.getElementById("sex_code2").disabled = true;
        document.getElementById("sex_code2").selectedIndex = 0;
        document.getElementById("eligibility2").disabled = true;
        document.getElementById("eligibility2").selectedIndex = 0;
    };

    /* Age and Comparison Age Option Forced to "Any" for Unsmoothed or Smoothed Age Standardized Measures */
    if (document.getElementById("adjust").value === "2" || document.getElementById("adjust").value === "4") {
        document.getElementById("age_group").value = "null";
        document.getElementById("age_group").disabled = true;
        document.getElementById("age_group2").value = "null";
        document.getElementById("age_group2").disabled = true
    } else {
        if (ms=== "1"|| ms=== "2" || ms=== "3" || ms=== "4" || ms=== "5" || ms=== "6" || ms=== "7"){
            document.getElementById("age_group").disabled = true;
        }
        else if (ms!="o" && ms != "u"){
            document.getElementById("age_group").disabled = false;
        } 
    };

    if (document.getElementById("geography").value === "s") {
        d3.select(".profile").style("display", "none");
    } else {
        d3.select(".profile").style("display", "");
    }

    if (tp==="m" || document.getElementById("condition").value==='144'){
        updateMenu(d3.select("#race_code"), race_edit);
        updateMenu(d3.select("#race_code2"), race_edit);
        if(document.getElementById("sex_code").value!='null'){
            document.getElementById("age_group").disabled = true
            document.getElementById("age_group").value = 'null'
            document.getElementById("race_code").disabled = true
            document.getElementById("race_code").value = 'null'
            document.getElementById("dual").disabled = true
            document.getElementById("dual").value = 'null'
            document.getElementById("eligibility").disabled = true
            document.getElementById("eligibility").value = 'null'
    
        }
        else if(document.getElementById("age_group").value!='null'){
            document.getElementById("sex_code").disabled = true
            document.getElementById("sex_code").value = 'null'
            document.getElementById("race_code").disabled = true
            document.getElementById("race_code").value = 'null'
            document.getElementById("dual").disabled = true
            document.getElementById("dual").value = 'null'
            document.getElementById("eligibility").disabled = true
            document.getElementById("eligibility").value = 'null'
        }
        else if(document.getElementById("race_code").value!='null'){
            document.getElementById("sex_code").disabled = true
            document.getElementById("sex_code").value = 'null'
            document.getElementById("age_group").disabled = true
            document.getElementById("age_group").value = 'null'
            document.getElementById("dual").disabled = true
            document.getElementById("dual").value = 'null'
            document.getElementById("eligibility").disabled = true
            document.getElementById("eligibility").value = 'null'
        }
        else if(document.getElementById("dual").value!='null'){
            document.getElementById("sex_code").disabled = true
            document.getElementById("sex_code").value = 'null'
            document.getElementById("age_group").disabled = true
            document.getElementById("age_group").value = 'null'
            document.getElementById("race_code").disabled = true
            document.getElementById("race_code").value = 'null'
            document.getElementById("eligibility").disabled = true
            document.getElementById("eligibility").value = 'null'
        }
        else if(document.getElementById("eligibility").value!='null'){
            document.getElementById("sex_code").disabled = true
            document.getElementById("sex_code").value = 'null'
            document.getElementById("age_group").disabled = true
            document.getElementById("age_group").value = 'null'
            document.getElementById("race_code").disabled = true
            document.getElementById("race_code").value = 'null'
            document.getElementById("dual").disabled = true
            document.getElementById("dual").value = 'null'
        }
        else{
            if (document.getElementById("adjust").value === "2" || document.getElementById("adjust").value === "4") {
                document.getElementById("age_group").value = "null";
                document.getElementById("age_group").disabled = true;
                document.getElementById("sex_code").disabled = false
                document.getElementById("race_code").disabled = false
                document.getElementById("dual").disabled = false
                document.getElementById("eligibility").disabled = false
            }
            else {
                document.getElementById("sex_code").disabled = false
                document.getElementById("age_group").disabled = false
                document.getElementById("race_code").disabled = false
                document.getElementById("dual").disabled = false
                document.getElementById("eligibility").disabled = false
            }
        }
        if (document.getElementById("analysis").value !== "base") {
            //Not base analysis
            if(document.getElementById("sex_code2").value!='null'){
                //Greys out filters
                document.getElementById("age_group2").disabled = true
                document.getElementById("age_group2").value = 'null'
                document.getElementById("race_code2").disabled = true
                document.getElementById("race_code2").value = 'null'
                document.getElementById("dual2").disabled = true
                document.getElementById("dual2").value = 'null'
                document.getElementById("eligibility2").disabled = true
                document.getElementById("eligibility2").value = 'null'
        
            }
            else if(document.getElementById("age_group2").value!='null'){
                document.getElementById("sex_code2").disabled = true
                document.getElementById("sex_code2").value = 'null'
                document.getElementById("race_code2").disabled = true
                document.getElementById("race_code2").value = 'null'
                document.getElementById("dual2").disabled = true
                document.getElementById("dual").value = 'null'
                document.getElementById("eligibility2").disabled = true
                document.getElementById("eligibility2").value = 'null'
            }
            else if(document.getElementById("race_code2").value!='null'){
                document.getElementById("sex_code2").disabled = true
                document.getElementById("sex_code2").value = 'null'
                document.getElementById("age_group2").disabled = true
                document.getElementById("age_group2").value = 'null'
                document.getElementById("dual2").disabled = true
                document.getElementById("dual2").value = 'null'
                document.getElementById("eligibility2").disabled = true
                document.getElementById("eligibility2").value = 'null'
            }
            else if(document.getElementById("dual2").value!='null'){
                document.getElementById("sex_code2").disabled = true
                document.getElementById("sex_code2").value = 'null'
                document.getElementById("age_group2").disabled = true
                document.getElementById("age_group2").value = 'null'
                document.getElementById("race_code2").disabled = true
                document.getElementById("race_code2").value = 'null'
                document.getElementById("eligibility2").disabled = true
                document.getElementById("eligibility2").value = 'null'
            }
            else if(document.getElementById("eligibility2").value!='null'){
                document.getElementById("sex_code2").disabled = true
                document.getElementById("sex_code2").value = 'null'
                document.getElementById("age_group2").disabled = true
                document.getElementById("age_group2").value = 'null'
                document.getElementById("race_code2").disabled = true
                document.getElementById("race_code2").value = 'null'
                document.getElementById("dual2").disabled = true
                document.getElementById("dual2").value = 'null'
            }
            else{
                if (document.getElementById("adjust").value === "2" || document.getElementById("adjust").value === "4") {
                    //document.getElementById("age_group").value = "null";
                    //document.getElementById("age_group").disabled = true;
                    document.getElementById("age_group2").value = "null";
                    document.getElementById("age_group2").disabled = true
                    document.getElementById("sex_code2").disabled = false
                    document.getElementById("race_code2").disabled = false
                    document.getElementById("dual2").disabled = false
                    document.getElementById("eligibility2").disabled = false
                }
                else {
                    document.getElementById("sex_code2").disabled = false
                    document.getElementById("age_group2").disabled = false
                    document.getElementById("race_code2").disabled = false
                    document.getElementById("dual2").disabled = false
                    document.getElementById("eligibility2").disabled = false
            }
        }
        }
    }

    var options = {};
    var option_list = [];
    menus.forEach(function(d) {
        options[d.id] = document.getElementById(d.id).value;
        options[d.id] = options[d.id] === "null" ? "." : options[d.id];

        //Arrow implies order matters, and : only 2 possible values, start and finish. We can have populate validate any expectations
        if (options[d.id] && range_splitter(options[d.id]).length > 1) {
            //The user chose multiple options; the populate function's responsibility to figure out how to handle that
            options[d.id] = range_splitter(options[d.id]);
        }
    });
    return options;
};

var range_splitter = function(to_be_split, split_on){
  split_on = split_on || '->';
  return to_be_split.split(split_on);
}

var int_parser = function(_){
  //For some reason passing parseInt func to map, etc isn't  allowed;
  //like binding native events console.log to event handlers - also not allowed

  return parseInt(_);
}

//ranged_splitter_ints
var ranged = function(range_ints) { //to_be_split, split_on){
  //http://stackoverflow.com/a/8069367/1175496
  var results = [];
  //var range_ints = range_splitter_ints(to_be_split, split_on);

  var lowEnd = range_ints[0];
  var highEnd = range_ints[1];
//Splits years for trend
  for (var i = lowEnd; i <= highEnd; i++) {
      if (i>9 && i<20){
          continue;
      }
      results.push(i);
  }

  return results;
}
//Gets years for FFS trend
var get_trend_year_option = function(){
    return {'value': '2->23'};
}

//Gets years for MA trend
var get_trend_year_option_MA = function(){
    return {'value': '8->9'};
}