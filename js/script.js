$(function() {
    // TOC Stuff
    $(".col-sm-9 > #markdown-toc").addClass('nav').appendTo(".sidebar #active-menu");

    $('.section > h1').each(function(i, e){
      var menu = $(".bs-docs-sidenav a[href='#" + e.id + "']").parent();
      $(e).next().find('#markdown-toc').addClass('nav').appendTo(menu);
    });

    // Sidenav affixing
        setTimeout(function () {
          var $sideBar = $('.bs-docs-sidebar')

          $sideBar.affix({
            offset: {
              top: function () {
                var offsetTop      = $sideBar.offset().top
                var sideBarMargin  = parseInt($sideBar.children(0).css('margin-top'), 10)
                var navOuterHeight = $('.navbar').height()

                return (this.top = offsetTop - navOuterHeight - sideBarMargin)
              },
              bottom: function () {
                return (this.bottom = $('.bs-docs-footer').outerHeight(true))
              }
            }
          });

          $('body').scrollspy({ target: '.bs-docs-sidebar' })
        }, 100)
});
