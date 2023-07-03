// Define the width and height of our chart
var width = 960,
    height = 350;

// Define the y scale, which is linear and maps to between the range of the height of the chart and 0
var y = d3.scale.linear()
    .range([height, 0]);
// We define the domain once we get our data in d3.json, below

// Our chart object is defined using the height and width
var chart = d3.select(".chart")
    .attr("width", width)
    .attr("height", height);

// We fetch the JSON from our controller, then process the resulting data
d3.json("/total_flights.json", function (data) {

    // We define colors for the bars - one for default, one for the mode
    var defaultColor = 'steelblue';
    var modeColor = '#4CA9F5';

    // We compute the maximum value for the bars, then set the domain for the y axis.
    // This means that y will now map from [0 -> maxY] to [height -> 0].
    var maxY = d3.max(data, function (d) { return d.total_flights; });
    y.domain([0, maxY]);

    // Color the bar with the maximum value, the mode, differently
    var varColor = function (d, i) {
        if (d['total_flights'] == maxY) { return modeColor; }
        else { return defaultColor; }
    }

    // Divide the width by the number of bars to get the bar width
    var barWidth = width / data.length;

    // We create our bar set of SVG containers (g elements) with attached data (the total_flights)
    // where each one is offset (transalted) by the barWidth of its index in the list of data values.
    var bar = chart.selectAll("g")
        .data(data)
        .enter()
        .append("g")
        .attr("transform", function (d, i) { return "translate(" + i * barWidth + ",0)"; });

    // Now we define a rectangle for each container with the height mapped from the total_flights data point
    // to the y axis, and the width barWidth - 1 pixel. We will it with the bar color.
    // Plug in varColor to color the mode's bar differently
    bar.append("rect")
        .attr("y", function (d) { return y(d.total_flights); })
        .attr("height", function (d) { return height - y(d.total_flights); })
        .attr("width", barWidth - 1)
        .style("fill", varColor);

    // We then label each bar with a the raw value in the top middle of the bar.
    // We offset the label by 3 to make it under the end of the bar, in the blue bit and we make it white
    // to stand out from the blue using the CSS from the HTML template above for text.
    bar.append("text")
        .attr("x", barWidth / 2)
        .attr("y", function (d) { return y(d.total_flights) + 3; })
        .attr("dy", ".75em")
        .text(function (d) { return d.total_flights; });
});