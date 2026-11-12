var currently_left = true;

function toTitleCase(str) {
    if(str){
        return str.replace(/\w\S*/g, function(txt) {
            return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();
        });
    }
}

var tooltip_is_pinned = false;
var pinned_jurisdiction_id = undefined;
var pinned_jursdiction_options = undefined;

var pin_tooltip = function(true_if_pinned, jurisdiction_id, options) {

    if (true_if_pinned) {
        tooltip_is_pinned = true;
        pinned_jurisdiction_id = jurisdiction_id;
        pinned_jursdiction_options = options;

        //Not sure what "mode" parameter (3rd parameter) does?

    } else {
        tooltip_is_pinned = false;
        pinned_jurisdiction_id = undefined;
        pinned_jursdiction_options = undefined;
        //Just use CSSdeactivate_tooltip_links();
    }

    d_tooltip_container
        .classed({
            'pinned': tooltip_is_pinned
        });

    tooltip(jurisdiction_id, options, "???");
}

//https://jira.us.kworld.kpmg.com/secure/RapidBoard.jspa?rapidView=398&projectKey=NORCCMS&view=planning&selectedIssue=NORCCMS-26
var last_tooltiped_jurisdiction = undefined;

var get_option = {
  analysis : {
    is_base : function(options){
      return options.analysis === 'base';
    }
  }
}

//TODO: use this so we have a function whose usages we can find, rather than text we must search
var floor_div_k = function(d){
  //Some cases (tooltip was being redrawn) I was attempting to re-floor_div_k
    //1) using "last_hovered_jurisdiction_id", which was hovered over a state jurisdiction (50), even though user changed to options.geography = 'c'
    //2) having trend view open and downgrading from State to County
  //a state's jurisdiction value unnecessarily
  //So now I only floor when it makes sense:
  var result =  (d >= 1000) ?  Math.floor(d / 1000) : d;

  return result;
}

var get_comparable_juris_id = function(d, options){

    if (d===undefined || d===null){
      return d;
    }

    //Can get current options
    //Consider whether the jurisdiction iterator would help here
    options = options || menuChange();

    // State vs. County View Adjustment
    if (options.geography === "s") {
        d = floor_div_k(d);
    };

    return d;
  }

function tooltip(hovered_jurisdiction_id, options, mode) {

    //https://jira.us.kworld.kpmg.com/projects/NORCCMS/issues/NORCCMS-34?filter=allissues
    hovered_jurisdiction_id = get_comparable_juris_id(hovered_jurisdiction_id);

    different_jurisdiction = last_tooltiped_jurisdiction != hovered_jurisdiction_id;

    if (hovered_jurisdiction_id!==undefined && hovered_jurisdiction_id!==null && !tooltip_is_pinned) {
        last_tooltiped_jurisdiction = hovered_jurisdiction_id;
    }


    if (tooltip_is_pinned) {
        hovered_jurisdiction_id = last_tooltiped_jurisdiction;
        //Leave it alone?
        //Pinning has two effects:
        //Leaving the box visible; i.e. not making invisible
        //Leaving the jurisdiction's data in that box; i.e. not changing its infotext
        //However the user may want to change what that data is; i.e. new Analysis, or new adjustment
        //Maybe pinning should only do one of those things; let's see...
        
        d_tooltip_container
            .classed({
                'showing': true
            });

        if (different_jurisdiction) {
            //https://jira.us.kworld.kpmg.com/browse/NORCCMS-17
            //returning here will prevent  hiding it; i.e. when it is pinned
            //In which case, different_urisdiction_id could be 1) Hovering over a new county or 2) mouseexit from a county to no county
            // console.info('Not updating the tooltip because it is a different jurisdiction')
            return;
        }
    }

    if (hovered_jurisdiction_id === null || hovered_jurisdiction_id === undefined) {
        //console.info('Hiding the tooltip because hovered_jurisdiction_id is null or undefined')
        d_tooltip_container
            .classed({
                'showing': false
            });
        return;
    };


    var years = is_trend(options) ? options.year : [options.year];

    var infotext = "";
    var wrap_in = function(s, class_name, element_name) {
        return "<" + element_name + " class=\"" + class_name + "\">" + s + "</" + element_name + ">"
    }
    var wrap_in_p = function(s, class_name) {
        return wrap_in(s, class_name, 'p')
    };
    var wrap_in_span = function(s, class_name) {
        return wrap_in(s, class_name, 'span')
    };
    var wrap_in_amount = function(s) {
        return wrap_in_span(s, "wrap_in_amount")
    }
    var wrap_in_measure = function(s) {
        return wrap_in_span(s, "wrap_in_measure")
    }
    var wrap_in_jurisdiction = function(s) {
        return wrap_in_span(s, "wrap_in_jurisdiction")
    }
    var wrap_in_ref = function(s) {
        return wrap_in_span(s, "wrap_in_ref")
    }
    var wrap_in_level_race = function(s) {
        return wrap_in_span(s, "wrap_in_level_race")
    }

    var year_queue = queue();

    ///Here we get from year[0]
    years.forEach(function(year) {

        year_opt = cloneObject(options);

        year_opt.year = year;

        year_queue
            .defer(parse_and_cache_years_json_data, year_opt);

        //End foreach years
    });

    //Just get the callback directly
    year_queue
        .awaitAll(function(error, trend_years_data) {

            if (error) {

                return console.error(error);

            } else {

                var d = hovered_jurisdiction_id;


                //TODO: Use d3 or jQuery or anything to build this DOM/HTML; only use string-appending where it is text; use d3 or jQuery to build the HTML around it.
                trend_years_data
                    //TODO: sort here; just move sort upstream so you only need to sort once
                    .forEach(function(data, year_index) {

                        //SETUP
                        //==============
                        var measure_name = get_condition_name();
                        var condition_name = get_measure_name();
                        var race =  get_race_text();
                        var race2 = get_race_text('race_code2');

                        var dual_ =get_dual_text();
                        var dual_2 =get_dual_text('dual2');
                        var age_ = get_age_text();
                        var age_2 = get_age_text('age_group2');
                        var sex_ = get_sexcode_text();
                        var sex_2= get_sexcode_text('sex_code2');

                        var tooltip1 = get_string_by_id(228) + " ";//"Race: "+race + ", Dual:  "+dual_+ ", Age:  "+ age_+", Sex: "+ sex_+ ", Condition:  ";
                      //  var tooltip1 = race + " "+dual_+ " "+ age_+" "+ sex_+ " ";
                        var tooltip2 = get_string_by_id(229) +  " ";//"Race: "+race2+", Dual:  "+dual_2+ ", Age:  "+ age_2+", Sex:  "+ sex_2+ ", Condition:  ";
                      //  var tooltip2 = race2+" "+dual_2+ " "+ age_2+" "+ sex_2+ " ";

                        //denom here represents "per _____".
                        //This denominator is different than the concept of "use the value for period n-1 in the denominator of a difference calculation"; affecting actual math in trend calculation
                        var show_denom = (options.measure === "v" | options.measure === "q" | options.measure === "h");

                        var data_point = data[d] || {};
                        var denom_1 = wrap_in_ref(show_denom ? " (" + get_string_by_id(117) + " " + abbreviations['denominator'][(get_option.analysis.is_base(options) ? data_point.D : data_point.D1)] + " beneficiaries)" : "");
                        var denom_2 = wrap_in_ref(show_denom ? " (" + get_string_by_id(117) + " " + abbreviations['denominator'][(get_option.analysis.is_base(options) ? null : data_point.D2)] + " beneficiaries)" : "");

                        //This never uses a trend formatter; so just pass the options.measure
                        var formatter = get_formatter(options.measure);
                        
                        var populationNote = {
                        '1': " " + get_string_by_id(false, false, { "english": "(U.S. adults aged 25 and older)", "spanish": "(Adultos estadounidenses mayores de 25 años)" }),
                        '2': " " + get_string_by_id(false, false, { "english": "(U.S. adults aged 65 and older)", "spanish": "(Adultos estadounidenses mayores de 65 años)" }),
                        '3': " " + get_string_by_id(false, false, { "english": "", "spanish": "" }),
                        '4': " " + get_string_by_id(false, false, { "english": "(U.S. adults and U.S. households)", "spanish": "(Adultos estadounidenses y hogares estadounidenses)" }),
                        '5': " " + get_string_by_id(false, false, { "english": "(U.S. households)", "spanish": "(hogares estadounidenses)" }),
                        '6': " " + get_string_by_id(false, false, { "english": "(U.S. households)", "spanish": "(hogares estadounidenses)" }),
                        '7': " " + get_string_by_id(false, false, { "english": "(U.S. households)", "spanish": "(hogares estadounidenses)" })
                        }
                        
                        popNote = populationNote[options.measure]
                        if (popNote == undefined){
                            popNote = ""
                        }

                        var local = wrap_in_amount(formatter(get_option.analysis.is_base(options) ? data_point.V : data_point.V1) + (options.condition==='144'? suffixesSCD[options.measure] :suffixes[options.measure])+popNote) + denom_1;
                        var comp = wrap_in_amount(formatter(get_option.analysis.is_base(options) ? null : data_point.V2) + (options.condition==='144'? suffixesSCD[options.measure] :suffixes[options.measure])) + denom_2;
                        var diff = wrap_in_amount(formatter(get_option.analysis.is_base(options) ? null : data_point.V) + (options.condition==='144'? suffixesSCD[options.measure] :suffixes[options.measure]));


                        function get_year_tooltip_part() {

                            result = '';

                            var current_year = years[year_index];

                            var current_year_label = get_year_label(current_year) || current_year;
                            if (current_year_label=="23"){
                                current_year_label= '2023';    
                            }

                            result = "<b>" + current_year_label + "</b>";

                            return wrap_in_p(result, 'year-label');

                        }

                        current_infotext = '';

                        //https://jira.us.kworld.kpmg.com/projects/NORCCMS/issues/NORCCMS-13?filter=allopenissues
                        current_infotext += get_year_tooltip_part();

                        function get_jurisdiction_label_part() {

                            result = '';


                            if (d === 66010 || d === 69000 || d === 60040 || floor_div_k(d) === 78 || d === 60 || d === 69 || d == 78 || d == 72 || d == 66) {
                                if (options.geography === "c") {
                                    //Remember: namesById  maps the county ID number (FIPS?) to their county names, title-cased
                                    //Or a 2-digit state ID number (FIPS / 1000) to their state names, uppercased
                                    d = floor_div_k(d);
                                    result += "<b>" + toTitleCase(namesById.get(d)) + "</b>" + " (Territorywide)";
                                } else {
                                    result += "<b>" + toTitleCase(namesById.get(d)) + " (Territorywide)" + "</b>";
                                };

                            } else {
                                //Known issue that
                                //Uncaught TypeError: Cannot read property 'replace' of undefined
                                //Is it same as this one?
                                //https://jira.us.kworld.kpmg.com/browse/NORCCMS-8
                                result += "<b>" +
                                    //If County; show  the County name
                                    toTitleCase(namesById.get(d)).replace("-", "&#8210") +
                                    " " + (options.geography === "s" ? " (Statewide)" : "") +
                                    //If County; Show the State name, like (Minnesota)
                                    "</b>" + (options.geography === "c" ? " (" + toTitleCase(namesById.get(floor_div_k(d))) + ")" : "")
                            }

                            return wrap_in_p(result, 'jurisdiction-label');
                        }

                        current_infotext += get_jurisdiction_label_part();

                        function get_condition_tooltip() {
                            result = '';
                            result += condition_name;
                            return wrap_in_p(result, 'condition-name');
                        }
                        current_infotext += get_condition_tooltip();

                        var sufficient = false;
                        if (get_option.analysis.is_base(options) && (d in data)) {
                            sufficient = true;
                        }
                        if (!get_option.analysis.is_base(options) && (d in data) && ('D1' in data[d]) && ('D2' in data[d])) {
                            sufficient = true;
                        }

                        if (sufficient) {
                            function get_first_measure_part() {

                                var measure_prefix_part = '';
                                if (options.analysis === "ruralurbandiff") {
                                   measure_prefix_part += "Urban Within ";
                               }

                                if (!get_option.analysis.is_base(options) && options.analysis !== "countydiff") {
                                    measure_prefix_part += (options.geography === "c" ? "County " : "State ");
                                }
                                if ( options.analysis !== "diffindiff" && options.measure!= 'o') {//options.race_code !== "." &&
                                  measure_prefix_part += tooltip1 + " ";
                                  //  measure_prefix_part += race + " ";
                                }
                                if (options.analysis === "diffindiff") {
                                  measure_prefix_part += tooltip1 + " minus " + tooltip2 + " ";
                                  //  measure_prefix_part += race + " minus " + race2 + " ";
                                }

                                var measure_part = wrap_in_measure(measure_prefix_part + measure_name);
                                
                                //Not gonna wrap it again, it's alreaddy been wrappedwrap_in_amount(local);
                                var result = measure_part + ": " + local;
                                if (options.measure ==="o"){
                                    if(options.geography==='c'){
                                        result = result + get_string_by_id("324") + get_string_by_id("322")
                                    }
                                    else{
                                        result = result + get_string_by_id("324") + get_string_by_id("323")
                                    }
                                }
                                //console.log(result)
                                return wrap_in_p(result, 'first-measure'); //result += wrap_in_level_race(level_race);

                            }
//Will need to append additional information for market saturation data
                            if (options.measure ==='o'){
                            //console.log(get_data(options)[d].upp);
                            //console.log(get_data(options)[d].beneficiaries);
                                if(get_data(options)[d].upp=== 'NaN'){
                                    current_infotext += get_first_measure_part()+get_string_by_id("319") + get_data(options)[d].beneficiaries+get_string_by_id("321");
                                }
                                else{
                                    current_infotext += get_first_measure_part()+get_string_by_id("319")+get_data(options)[d].upp +get_string_by_id("320")+get_data(options)[d].beneficiaries+get_string_by_id("321");
                                }
                                if(options.geography==='c'){
                                    current_infotext += get_string_by_id("322")
                                }
                                else{
                                    current_infotext +=  get_string_by_id("323")
                                }
                                }
                                else{
                                    current_infotext += get_first_measure_part();
                                }
                            

                            function get_second_measure_part() {
                                var measure_prefix_part = '';
                                var result = '';

                                if (options.analysis === "statediff") {
                                    measure_prefix_part += toTitleCase(namesById.get(floor_div_k(d))) + " ";
                                } else if (options.analysis === "nationdiff") {
                                    measure_prefix_part += "National ";
                                }else if (options.analysis === "ruralurbandiff") {
                                    measure_prefix_part += "Rural Within State ";
                                }


                                if (!get_option.analysis.is_base(options)) {
                                    if ( options.analysis !== "diffindiff") {//options.race_code2 !== "." &&
                                      measure_prefix_part += tooltip2 + " ";
                                      //  measure_prefix_part += race2 + " ";
                                    }
                                    if (options.analysis === "diffindiff") {

                                      measure_prefix_part += "State " + tooltip1 + " minus " + tooltip2 + " ";
                                      //  measure_prefix_part += "State " + race + " minus " + race2 + " ";
                                    }

                                    var measure_part = wrap_in_measure(measure_prefix_part + measure_name);
                                    result = measure_part + ": " + comp; // Already wrappedwrap_in_amount(comp);
                                }

                                return wrap_in_p(result, 'second-measure');

                            }


                            current_infotext += get_second_measure_part();

                            function get_third_measure_part(){

                              result = '';

                              if (!get_option.analysis.is_base(options)) {
                                  if (options.analysis === "diffindiff") {
                                      result += wrap_in_measure(get_string_by_id("260x") + " ") + ": " + diff; //Already wrapped wrap_in_amount(diff);
                                  } else {
                                      result += wrap_in_measure(get_string_by_id("259x") + " " + measure_name) + ": " + diff; //Already wrapped wrap_in_amount(diff);
                                  }
                              }
                              return wrap_in_p(result, 'third-measure');
                            }

                            current_infotext += get_third_measure_part();
                            //Sets tooltip to "Insufficient Data" when nonnumeric value is loaded for the jurisdiction ("NaN").
                            if (options.analysis!=="ruralurbandiff"){
                                try{if (isNaN(get_data(options)[d].V)){
                                    current_infotext = ""
                                    current_infotext+= get_year_tooltip_part() + get_jurisdiction_label_part() + get_condition_tooltip();
                                    current_infotext += "<i>" + get_string_by_id(false, false, { "english": "Insufficient Data", "spanish": "Datos insuficientes"}) + "</i>";
                                }}
                                catch(e){
                                    
                                }
                            }

                        } else {
                            current_infotext += "<i>" + get_string_by_id(false, false, { "english": "Insufficient Data", "spanish": "Datos insuficientes"}) + "</i>";
                        };

                        //TODO the % change part is repeated in the legend
                        infotext += "<div class=\"year-panel " + (is_trend(options) ? 'trend' : '') + "\">" + current_infotext + "</div>";

                    })


                var trend_data = get_data(options);

                //The above concatenation of tooltips; (i.e. class="year-panel") are the individual values that went into the trend calculation
                //This will show the trend value
                if (is_trend(options)) {

                    if (!trend_data || !trend_data[d]) {

                        console.error('No trend_data even though I expect trends while trying to write tooltip');

                    } else {
                        //We already know it's a trend
                        //TODO: Avoid the br
                        
                        infotext += "<div class=\"year-panel trend\"> " +
                              wrap_in_p('<b>Trend </b>', 'trend-label') +
                              wrap_in_p(get_formatter(options)(trend_data[d].V) + '%', 'trend-value') +
                              wrap_in_ref('(% change from ' + options.year.map(get_year_label).join('-') + ')') +
                            "</div>"

                    }

                }

                //https://jira.us.kworld.kpmg.com/browse/NORCCMS-37
                var juris_type = options.geography == 's' ? 'state' : 'county'
                if (juris_type === 'county'){
                    var hint_msg = get_string_by_id("125");
                }else{var hint_msg = get_string_by_id("342");}

                infotext += wrap_in_p(hint_msg, 'hint');

                //This needs to wait
                //console.log('writing HTML for tooltip')
                d_tooltip
                .classed({'trend' : is_trend(options)})
                .html(infotext);


            }
        });

    //
    // //This code doesn't need to wait until the infotext is finished being composed
    //

    // var map_bounds = d3.select(".overlay").node().getBoundingClientRect();
    //
    // var hit = { 'x': null, 'y': null };
    // hit.y = 5;
    //
    // var mp = (map_bounds.right + map_bounds.left) / 2;
    //
    // if (currently_left && d3.event.pageX < mp - map_bounds.width / 6) {
    //     currently_left = false;
    // } else if (!currently_left && d3.event.pageX > mp + map_bounds.width / 6) {
    //     currently_left = true;
    // };
    //
    // if (currently_left) {
    //     hit.x = 5;
    // } else {
    //
    //     //Gotta show it before you take its size
    //     d_tooltip_container
    //         .classed({ 'showing': true });
    //
    //     var size = d_tooltip_container.node().getBoundingClientRect();
    //
    //     d_tooltip_container
    //         .style({
    //             "left": "0px"
    //         });
    //       //Keeps getting bigger and bigger by 10 px
    //     hit.x = map_bounds.width - size.width - 10;
    // };

    d_tooltip_container
        .classed({
            'showing': true
        });
    // .style({
    //     "left": hit.x + "px",
    //     "top": hit.y + "px"
    // });

};